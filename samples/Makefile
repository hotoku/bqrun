.PHONY: all
all: done.1


done.1: 1.sql
	cat 1.sql | bq query
	touch $@
