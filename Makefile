GIT_VERSION ?= ${shell git describe --tags --long --always}
RPM_VERSION ?= ${shell echo $(GIT_VERSION) | cut -b2- | cut -d"-" -f1}
RPM_RELEASE ?= ${shell echo $(GIT_VERSION) | cut -d"-" -f2}

docker: package FORCE
	docker build . -t cloudwatch-connector:$(RPM_VERSION)_$(RPM_RELEASE) -f docker/alpine/Dockerfile

package: clean FORCE
	mvn -q clean install package -DskipTests

test: clean FORCE
	mvn -q clean install test
	
clean: FORCE
	mvn -q -o clean

FORCE:
