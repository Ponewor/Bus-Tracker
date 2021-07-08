FROM gradle:7.1-jdk8

COPY build.gradle .

WORKDIR /home/gradle/src

COPY src .

WORKDIR /home/gradle

RUN gradle build

#CMD touch xd.txt && tail -f xd.txt
CMD java -jar build/libs/gradle-all.jar
