#FROM python:3.10.13-bullseye
FROM python:3.10.15-bookworm

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y \
    default-jre \
    rsync \
    git \
    nano \
    vim \
    less \
    libxml2-dev \
    python3 \
    build-essential \
    make \
    gcc \
    python3-dev \
    locales \
    python3-pip \
    ruby-dev \
    rubygems \
    poppler-utils \
    python-is-python3 && \
    gem install sass && \
    pip install --upgrade pip && \
    apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

COPY ./requirements/ /tmp/requirements/
RUN pip install --use-deprecated=legacy-resolver -r /tmp/requirements/dev.txt

RUN mkdir /copo
RUN mkdir /code

WORKDIR /copo
COPY . /code/

RUN wget -O webin-cli.jar https://github.com/enasequence/webin-cli/releases/download/8.0.0/webin-cli-8.0.0.jar

# add aspera client to path
COPY ./shared_tools/reposit/.aspera/ /root/.aspera/
ENV PATH /root/.aspera/cli/bin:$PATH

ENTRYPOINT ["bash","-c","rsync -avhW --no-compress --progress /code/ /copo/ &&  rm -rf /code/ && python manage.py migrate && python manage.py social_accounts && python manage.py setup_groups && python manage.py setup_schemas && python manage.py createcachetable && python manage.py collectstatic --noinput && supervisord -c src/celery.conf && supervisorctl -c src/celery.conf start all && /usr/local/bin/daphne -b 0.0.0.0 -p 8000 src.main_config.asgi:application"]

# wget -nv -O- https://download.calibre-ebook.com/linux-installer.sh | sh /dev/stdin &&
