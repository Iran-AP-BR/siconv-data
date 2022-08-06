FROM python:3.8

ARG API_USER=api_user

USER root
RUN  echo "PS1='\u @ \e[33msiconv-data-api\e[m \e[36m\w \$\e[m '" >> /etc/bash.bashrc
RUN adduser ${API_USER}

USER ${API_USER}
ENV VIRTUAL_ENV=/home/${API_USER}/venv
RUN python -m venv ${VIRTUAL_ENV}
ENV PATH=${VIRTUAL_ENV}/bin:${PATH}

WORKDIR /home/${API_USER}/app
COPY ./requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "run.py" ]
