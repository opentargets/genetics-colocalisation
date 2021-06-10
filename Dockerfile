FROM r-base:3.6.1

# Create non-root user 
ARG UID
ARG GID
RUN groupadd -g $GID -o otg
RUN useradd -m -u $UID -g $GID -o -s /bin/bash otg

# Do everything that requires root user
# Install OpenJDK-8
RUN apt-get update && \
    apt-get remove -y -o APT::Immediate-Configure=0 libgcc1 && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# curl
RUN apt-get install -y curl

# Install parallel
RUN apt install -yf parallel

# download gcta
RUN apt-get install unzip
RUN wget https://cnsgenomics.com/software/gcta/bin/gcta_1.92.3beta3.zip -P /software/gcta
RUN chown -R otg:otg /software/gcta

# Install R packages
RUN apt-get -y install libxml2-dev libssl-dev libcurl4-openssl-dev
RUN Rscript -e "install.packages('BiocManager', dependencies=TRUE, repos='http://cran.rstudio.com/')" -e "BiocManager::install(c('snpStats'))"
RUN R -e "install.packages('coloc', dependencies=TRUE, repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('tidyverse', dependencies=TRUE, repos='http://cran.rstudio.com/')"

# switch to otg user
USER otg

ENV credset_dir='/data/credset'
ENV DEBIAN_FRONTEND=noninteractive

# Conda and the envirounment dependencies
COPY ./environment.yaml /home/otg/coloc/
WORKDIR /home/otg/coloc
RUN mkdir /home/otg/conda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /home/otg/conda/miniconda.sh
RUN bash /home/otg/conda/miniconda.sh -b -p /home/otg/conda/miniconda
ENV PATH="/home/otg/conda/miniconda/bin:${PATH}"
RUN conda env create -n coloc --file environment.yaml

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME='/home/otg/conda/miniconda/envs/coloc'

# Default command
CMD ["/bin/bash"]

# Install GCTA
RUN unzip /software/gcta/gcta_1.92.3beta3.zip -d /software/gcta
RUN rm /software/gcta/gcta_1.92.3beta3.zip
ENV PATH="/software/gcta/gcta_1.92.3beta3:${PATH}"

# Google Cloud SDK
RUN curl https://sdk.cloud.google.com | bash

# Copy the v2d project
COPY ./ /home/otg/coloc

# Make all files in coloc and data owned by the non-root user
USER root
RUN chown -R otg:otg /home/otg/coloc
RUN mkdir /data
RUN chown -R otg:otg /data
USER otg

# Activate coloc environment
ENV PATH /home/otg/conda/miniconda/envs/coloc/bin:$PATH
