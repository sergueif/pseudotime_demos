FROM debian:bookworm

RUN apt-get update && apt-get install -y sqlite3 ruby ruby-sqlite3 libsqlite3-dev ruby-dev build-essential pkg-config

RUN gem install sequel sqlite3

RUN mkdir /pt_demo

WORKDIR /pt_demo

COPY * /pt_demo/

CMD ["ruby", "todo_pseudo_time.rb"]
