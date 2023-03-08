FROM openjdk:8

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

#Install Scala
RUN wget https://downloads.lightbend.com/scala/2.13.5/scala-2.13.5.tgz && \
    tar -zxvf scala-2.13.5.tgz && \
    mv scala-2.13.5 /usr/lib && \
    rm scala-2.13.5.tgz

ENV PATH="$PATH:/usr/lib/scala-2.13.5/bin"

RUN mkdir /app

# Copy your Scala code to the container
COPY src/main/scala/Challenge.scala /app
COPY build.sbt /app

WORKDIR /app

RUN apt-get update && apt-get install -y curl bash

# Install sbt
RUN curl -sL "https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz" | tar xz -C /usr/local --strip-components=1 sbt/bin sbt/conf

# Compile sbt
RUN sbt compile

# Run Scala object
CMD ["sbt", "runMain Challenge"]
