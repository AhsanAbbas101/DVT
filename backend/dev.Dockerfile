FROM python

WORKDIR /usr/src/app

COPY requirements.txt .

RUN apt-get update && apt-get install -y libgl1 libglib2.0-0 && \
  rm -rf /var/lib/apt/lists/* && \
  pip install -r requirements.txt

CMD /bin/sh