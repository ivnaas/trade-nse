# Download ubuntu from docker hub
FROM ubuntu:latest

# Download updates and install python3, pip and vim
RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
RUN apt-get install vim -y
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*
RUN ln -fs /usr/share/zoneinfo/Asia/Kolkata /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata

#RUN echo "Asia/Kolkata" > /etc/timezone
#RUN dpkg-reconfigure -f noninteractive tzdata

#ENTRYPOINT ["python3"]
CMD [ "python3 --version"]
# Declaring working directory in our container
WORKDIR /opt/apps
CMD ["mkdir", "logs"]
# Copy all relevant files to our working dir /opt/apps/flaskap
COPY requirements.txt .

# Install all requirements for our app
RUN pip3 install -r requirements.txt

# Copy source files to $WORKDIR
COPY ./src .
RUN chmod +x nseindexshort.py
# Expose container port to outside host
EXPOSE 9000

# Run the application
CMD [ "python3", "/opt/apps/nseindexshort.py" ]
