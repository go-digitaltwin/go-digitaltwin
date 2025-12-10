/*
Package dbtest provides a convenient way to spin up a database container for
testing purposes. It provides a higher-level interface to the testcontainers-go
library that is suitable for common use-cases.

If you find yourself wanting to use a database container in a test and the
details of the database container are not important, you should use this
package. If, however, you need a specific customisation of the database, you
should use the testcontainers-go modules directly.

Developing locally with Docker, you may want to manually inspect the database
after a test failure. To do this, set the Inspect flag to true:

	go test -dbtest.inspect

This package is intended to be used in tests only. It is not suitable for
production use.
*/
package dbtest
