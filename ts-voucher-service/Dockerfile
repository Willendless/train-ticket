FROM python:3

RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt ./
RUN pip install --proxy http://proxy.pdl.cmu.edu:3128 cryptography
RUN pip install --proxy http://proxy.pdl.cmu.edu:3128 --no-cache-dir -r requirements.txt

ADD ./server.py /app/
CMD [ "python", "server.py" ]

EXPOSE 16101
