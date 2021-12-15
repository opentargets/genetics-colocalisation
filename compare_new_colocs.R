suppressMessages(library(tidyverse))
theme_set(theme_bw())

# The purpose of this script is to compare new coloc test results that overlap
# that overlap with those computed previously. When running the step to merge
# new coloc tests with those previously done, it's possible that some tests were
# re-computed, potentially to fix a problem in the previous version. To ensure
# that nothing strange is happening, we plot the new vs old colocs.
df = read_csv("coloc_raw.merged.csv")

p = ggplot(df, aes(x=prev_coloc_h4, y=new_coloc_h4)) +
  geom_point(alpha=0.1) +
  xlab("H4 - previously") +
  ylab("H4 - new") +
  ggtitle("Coloc results: new vs. previous")

print(p)

png("compare_new_colocs.png", width = 1000, height = 700)
print(p + theme_bw(16))
dev.off()
