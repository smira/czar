coverage:
	gocov test ./... | gocov-html > coverage.html
	open coverage.html

check:
	go tool vet -all=true .
	golint .