FROM r-base:3.6.1

ENV credset_dir='/data/credset'
ENV DEBIAN_FRONTEND=noninteractive

# Install OpenJDK-8
RUN apt-get update && \
    apt-get remove -y -o APT::Immediate-Configure=0 libgcc1 && \
    apt-get install -y ant && \
    apt-get clean;

# Conda and the envirounment dependencies
RUN mkdir /conda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /conda/miniconda.sh
RUN bash /conda/miniconda.sh -b -p /conda/miniconda
ENV PATH="/conda/miniconda/bin:${PATH}"
COPY ./environment.yaml /coloc/
WORKDIR /coloc
RUN conda env create -n coloc --file environment.yaml

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64/'

# Google Cloud SDK
RUN apt-get install -y curl
RUN curl https://sdk.cloud.google.com | bash

# Default command
CMD ["/bin/bash"]

# Install parallel
RUN apt install -yf parallel

# Install GCTA
RUN apt-get install unzip
RUN wget https://cnsgenomics.com/software/gcta/bin/gcta_1.92.3beta3.zip -P /software/gcta
RUN unzip /software/gcta/gcta_1.92.3beta3.zip -d /software/gcta
RUN rm /software/gcta/gcta_1.92.3beta3.zip
ENV PATH="/software/gcta/gcta_1.92.3beta3:${PATH}"

# Install R packages
RUN apt-get -y install libxml2-dev libssl-dev libcurl4-openssl-dev
RUN Rscript -e "install.packages('BiocManager', dependencies=TRUE, repos='http://cran.rstudio.com/')" -e "BiocManager::install(c('snpStats'))"
RUN R -e "install.packages('coloc', dependencies=TRUE, repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('tidyverse', dependencies=TRUE, repos='http://cran.rstudio.com/')"

# Copy the v2d project
COPY ./ /coloc

# Activate coloc environment
ENV PATH /conda/miniconda/envs/coloc/bin:$PATH