# pull official base image
FROM --platform=linux/amd64 continuumio/miniconda3 AS build

# set work directory
# WORKDIR /usr/src/app

# # set environment variables
# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONUNBUFFERED 1

# Install base utilities
# RUN apt-get update && \
#     apt-get install -y build-essential  && \
#     apt-get install -y wget && \
#     # The following line to unfuck cv2 because...cv2
#     apt-get install -y ffmpeg libsm6 libxext6 && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# # Install miniconda
# RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py39_4.9.2-Linux-x86_64.sh -O ~/miniconda.sh && \
#     /bin/bash ~/miniconda.sh -b -p /opt/conda && \
#     rm ~/miniconda.sh && \
#     /opt/conda/bin/conda clean -tipsy && \
#     ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
#     echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
#     echo "conda activate base" >> ~/.bashrc

# ENV PATH /opt/conda/bin:$PATH

# SHELL ["/bin/bash", "-c"]

COPY ./environment.yml .
RUN conda env create -f environment.yml

# install dependencies
RUN conda run -n xv2_backend pip install --upgrade pip
COPY ./requirements.txt .
RUN conda run -n xv2_backend pip install -r requirements.txt

# Install conda-pack:
RUN conda install -c conda-forge conda-pack

# Use conda-pack to create a standalone enviornment
# in /venv:
RUN conda-pack -n xv2_backend -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# We've put venv in same path it'll be in final image,
# so now fix up paths:
RUN /venv/bin/conda-unpack

FROM --platform=linux/amd64 python:3.9.5-slim as runtime

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# Install base utilities
RUN apt-get update && \
    apt-get install -y build-essential  && \
    apt-get install -y wget && \
    # The following line to unfuck cv2 because...cv2
    apt-get install -y ffmpeg libsm6 libxext6 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# copy project
COPY . .

# When image is run, run the code with the environment
# activated:
SHELL ["/bin/bash", "-c"]
ENTRYPOINT [ "source", "/venv/bin/activate" ]
