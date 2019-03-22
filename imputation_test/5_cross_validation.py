#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import pandas as pd
from glob import glob
import numpy as np
import random

# Sklearn
from sklearn import linear_model
from sklearn import ensemble
from sklearn import svm
from sklearn import model_selection
from sklearn import metrics

# Plot
import matplotlib.pyplot as plt

def main():

    # Non normalised files
    in_features = glob('output/features.csv/part-*.csv')[0]
    outdir = 'results'

    # Args
    fold_num = 5
    ncores = 1
    feature_cols = ['total_overlap',
                    'prop_overlap_0.1', 'prop_overlap_0.2', 'prop_overlap_0.3',
                    'prop_overlap_0.4', 'prop_overlap_0.5', 'prop_overlap_0.6',
                    'prop_overlap_0.7', 'prop_overlap_0.75', 'prop_overlap_0.8',
                    'prop_overlap_0.85', 'prop_overlap_0.9', 'prop_overlap_0.95',
                    'prop_overlap_0.975', 'prop_overlap_1.0', 'left_num_tags',
                    'right_num_tags', 'left_prop_total_overlap', 'right_prop_total_overlap',
                    'abs_distance', 'left_log_pval', 'right_log_pval', 'left_is_gwas',
                    'right_is_gwas']

    # Make output dir
    os.makedirs(outdir, exist_ok=True)

    # Load data
    df = (
        pd.read_csv(in_features, sep=',')
          .sample(frac=1, random_state=123)
    )

    # Create dict of regressors
    regressors = {
        'OLS': linear_model.LinearRegression(n_jobs=ncores),
        'RandomForest_100': ensemble.RandomForestRegressor(n_estimators=100, n_jobs=ncores),
        # 'SVR': svm.SVR(gamma='auto'),
        # 'GradientBoosting': ensemble.GradientBoostingRegressor()
    }


    # Iterate over right_types
    for right_type in ['all', 'gwas', 'molecular_trait']:

        # Take subset based on right_type
        if right_type == 'gwas':
            df_subset = df.query('right_type == "gwas"')
        elif right_type == 'molecular_trait':
            df_subset = df.query('right_type != "gwas"')
        elif right_type == 'all':
            df_subset = df
        
        # Extract features and outcomes
        X = df_subset.loc[:, feature_cols]
        y_dict = {
            'h3': df_subset['coloc_h3'],
            'h4': df_subset['coloc_h4'],
            'log_h4_h3': df_subset['coloc_log_H4_H3']
        }

        # Iterate over classifiers
        for rg_name, rg in regressors.items():

            # Iterate over outcomes
            for y_name, y in y_dict.items():

                if y_name.startswith('log'):
                    logscale = True
                else:
                    logscale = False

                # Perform cross validation regression
                outpref = os.path.join(outdir, '{0}.{1}.{2}'.format(rg_name, y_name, right_type))
                title = '{0} {1} {2}'.format(rg_name, y_name, right_type)
                perform_cross_validation(rg, y, X, fold_num, title, logscale, outpref)
                
                # Plot learning curve
                plot_learning_curve(rg, title, X, y, cv=fold_num)
                plt.tight_layout()
                plt.savefig(outpref + '.learning_curve.png', dpi=150)
                plt.close()

    return 0


def perform_cross_validation(rg, y, X, fold_num, title, logscale, outpref):

    # Initiate results
    r2_scores = []
    ft_imp = []
    y_test_all = []
    y_pred_all = []

    # Split data
    for train, test in model_selection.KFold(n_splits=fold_num).split(X, y):

        # Split
        X_train = X.iloc[train, :]
        y_train = y.iloc[train]
        X_test = X.iloc[test, :]
        y_test = y.iloc[test]

        # Fit model
        rg_fit = rg.fit(X_train, y_train)
        
        # Make predictions
        y_pred = rg_fit.predict(X_test)
        y_pred_all = y_pred_all + y_pred.tolist()
        y_test_all = y_test_all + y_test.tolist()

        # Score
        r2 = metrics.r2_score(y_test, y_pred)
        r2_scores.append(r2)

        # Extra feature importances
        try:
            ft_imp.append(rg_fit.feature_importances_)
        except AttributeError:
            pass
    
    # Print R2 results
    print(
        '{0}; R^2={1:.2f} ± {2:.2f}'.format(
            title, np.mean(r2_scores), np.std(r2_scores)
        ))

    # Plot scatter
    fig, ax = plt.subplots()
    plt.scatter(y_test_all, y_pred_all,
                color='black',
                alpha=0.1,
                marker='+')
    if logscale:
        plt.ylabel('Predicted value (log scale)')
        plt.xlabel('True value (log scale)')
    else:
        plt.ylabel('Predicted value')
        plt.xlabel('True value')
    lims = [np.min([ax.get_xlim(), ax.get_ylim()]),
            np.max([ax.get_xlim(), ax.get_ylim()])]
    ax.plot(lims, lims, '--', alpha=0.75, zorder=0)
    plt.title('True vs predicted value\n{0}\nR^2={1:.2f} ± {2:.2f}'.format(
        title, np.mean(r2_scores), np.std(r2_scores)))
    ax.set_xlim(lims)
    ax.set_ylim(lims)
    if logscale:
        ax.set_yscale('symlog')
        ax.set_xscale('symlog')
    plt.tight_layout()
    plt.savefig(outpref + '.scatter.png', dpi=150)
    plt.close()

    # Plot feature importances
    if len(ft_imp) > 0:
        # Calc means
        mean_ft_imp = np.mean(ft_imp, axis=0)
        std_ft_imp = np.std(ft_imp, axis=0)
        # Plot
        ind = np.arange(len(mean_ft_imp))
        width = 0.35
        plt.bar(ind, mean_ft_imp, width, yerr=std_ft_imp)
        plt.xticks(ind, X.columns, rotation='vertical')
        plt.ylabel('Feature importance')
        plt.title('Importance of features')
        plt.tight_layout()
        plt.savefig(outpref + '.ft_imp.png', dpi=150)
        plt.close()
    
    return 0

def plot_learning_curve(estimator, title, X, y, ylim=None, cv=None,
                        n_jobs=None, train_sizes=np.linspace(.1, 1.0, 5)):
    """
    Generate a simple plot of the test and training learning curve.

    Parameters
    ----------
    estimator : object type that implements the "fit" and "predict" methods
        An object of that type which is cloned for each validation.

    title : string
        Title for the chart.

    X : array-like, shape (n_samples, n_features)
        Training vector, where n_samples is the number of samples and
        n_features is the number of features.

    y : array-like, shape (n_samples) or (n_samples, n_features), optional
        Target relative to X for classification or regression;
        None for unsupervised learning.

    ylim : tuple, shape (ymin, ymax), optional
        Defines minimum and maximum yvalues plotted.

    cv : int, cross-validation generator or an iterable, optional
        Determines the cross-validation splitting strategy.
        Possible inputs for cv are:
          - None, to use the default 3-fold cross-validation,
          - integer, to specify the number of folds.
          - :term:`CV splitter`,
          - An iterable yielding (train, test) splits as arrays of indices.

        For integer/None inputs, if ``y`` is binary or multiclass,
        :class:`StratifiedKFold` used. If the estimator is not a classifier
        or if ``y`` is neither binary nor multiclass, :class:`KFold` is used.

        Refer :ref:`User Guide <cross_validation>` for the various
        cross-validators that can be used here.

    n_jobs : int or None, optional (default=None)
        Number of jobs to run in parallel.
        ``None`` means 1 unless in a :obj:`joblib.parallel_backend` context.
        ``-1`` means using all processors. See :term:`Glossary <n_jobs>`
        for more details.

    train_sizes : array-like, shape (n_ticks,), dtype float or int
        Relative or absolute numbers of training examples that will be used to
        generate the learning curve. If the dtype is float, it is regarded as a
        fraction of the maximum size of the training set (that is determined
        by the selected validation method), i.e. it has to be within (0, 1].
        Otherwise it is interpreted as absolute sizes of the training sets.
        Note that for classification the number of samples usually have to
        be big enough to contain at least one sample from each class.
        (default: np.linspace(0.1, 1.0, 5))
    """
    plt.figure()
    plt.title(title)
    if ylim is not None:
        plt.ylim(*ylim)
    plt.xlabel("Training examples")
    plt.ylabel("Score")
    train_sizes, train_scores, test_scores = model_selection.learning_curve(
        estimator, X, y, cv=cv, n_jobs=n_jobs, train_sizes=train_sizes)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)
    plt.grid()

    plt.fill_between(train_sizes, train_scores_mean - train_scores_std,
                     train_scores_mean + train_scores_std, alpha=0.1,
                     color="r")
    plt.fill_between(train_sizes, test_scores_mean - test_scores_std,
                     test_scores_mean + test_scores_std, alpha=0.1, color="g")
    plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
             label="Training score")
    plt.plot(train_sizes, test_scores_mean, 'o-', color="g",
             label="Cross-validation score")

    plt.legend(loc="best")
    return plt

if __name__ == '__main__':

    main()
