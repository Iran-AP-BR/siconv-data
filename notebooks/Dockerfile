FROM jupyter/base-notebook:latest

ENV VIRTUAL_ENV=/home/jovyan/venv

USER root
WORKDIR /home/jovyan
RUN rm work -R && conda install python=3.8.13 -y

USER jovyan
RUN python -m venv ${VIRTUAL_ENV}

ENV PATH=${VIRTUAL_ENV}/bin:${PATH}


WORKDIR /home/jovyan/notebooks
COPY ./requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir jupyterlab

COPY . .

WORKDIR /home/jovyan

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--NotebookApp.token=''","--NotebookApp.password=''"]
