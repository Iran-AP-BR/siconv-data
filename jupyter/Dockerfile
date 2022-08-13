FROM python:3.8

RUN apt-get update -y && \
    apt-get install tini -y

RUN  echo "PS1='\u @ \e[33msiconv-data-notebooks\e[m \e[36m\w \$\e[m '" >> /etc/bash.bashrc

RUN python -m pip install --upgrade pip && \
    pip install jupyterlab matplotlib

#WORKDIR /root/.jupyter/lab/user-settings/@jupyterlab/apputils-extension
#RUN echo "{\n\"theme\": \"JupyterLab Dark\",\n\"theme-scrollbars\": true\n}" > themes.jupyterlab-settings

WORKDIR /home/jupyter

COPY ./requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /home/jupyter

ENTRYPOINT ["tini", "--"]

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--NotebookApp.token=''","--NotebookApp.password=''", "--allow-root"]
