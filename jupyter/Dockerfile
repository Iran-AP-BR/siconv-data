FROM python:3.8

RUN apt-get update -y && \
    apt-get install tini -y

RUN  echo "PS1='\u @ \e[33msiconv-data-notebooks\e[m \e[36m\w \$\e[m '" >> /etc/bash.bashrc

RUN python -m pip install --upgrade pip && \
    pip install jupyterlab matplotlib

WORKDIR /home/jupyter

COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY ./user-settings/* /root/.jupyter/lab/user-settings/@jupyterlab

ENTRYPOINT ["tini", "--"]

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=''","--ServerApp.password=''", "--allow-root"]
