FROM --platform=linux/amd64 continuumio/miniconda3 AS build

COPY ./environment.yml .
RUN conda env create -f environment.yml

# install dependencies
RUN conda run -n xv2_backend pip install --upgrade pip
COPY ./requirements.txt .
RUN conda run -n xv2_backend pip install -r requirements.txt

RUN conda install -c conda-forge conda-pack

# Use conda-pack to create a standalone enviornment in /venv:
RUN conda-pack --ignore-missing-files -n xv2_backend -o env.tar
RUN mkdir /venv
RUN tar -xf env.tar -C /venv
RUN rm env.tar

# We've put venv in same path it'll be in final image,
# so now fix up paths:
RUN /venv/bin/conda-unpack

FROM --platform=linux/amd64 python:3.9.5-slim as runtime

# Install base utilities
RUN apt-get update && \
    # The following line to unfuck cv2 because...cv2
    apt-get install -y ffmpeg libsm6 libxext6 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# 'hactivate' our venv
ENV VIRTUAL_ENV=/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

SHELL ["/bin/bash", "-c"]

# copy project
COPY . .
