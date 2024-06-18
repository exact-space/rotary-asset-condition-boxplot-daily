FROM dev.exactspace.co/python3.8-base-es2:r1
COPY . /src/
RUN chmod +x /src/*
WORKDIR /src
ENTRYPOINT ["./main"]
#ENTRYPOINT ["python","index.py"]
