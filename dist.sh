#!/bin/bash
rpmbuild -bb client-metrics.spec --define "_sourcedir `pwd`"
