# Goblero 

Pure Go Simple, Embedded, Persistent Job Queue, backed by [BadgerDB](https://github.com/dgraph-io/badger)

[![Build Status](https://travis-ci.org/didil/goblero.svg?branch=master)](https://travis-ci.org/didil/goblero)
[![Coverage Status](https://coveralls.io/repos/github/didil/goblero/badge.svg?branch=master)](https://coveralls.io/github/didil/goblero?branch=master)
[![goreportcard](https://goreportcard.com/badge/github.com/didil/goblero)](https://goreportcard.com/report/github.com/didil/goblero)
[![codebeat badge](https://codebeat.co/badges/1d261e4f-36ff-42b5-b015-e31eb7aa7e7d)](https://codebeat.co/projects/github-com-didil-goblero-master)


**DO NOT USE IN PRODUCTION** This library is still a Work In Progress 

## About Goblero

Goblero is a simple, embedded, persistent job queue backed by BadgerDB. It can provided in-process job processing to any Go app.

## Usage 
The full API is documented on [godoc.org](https://godoc.org/github.com/didil/goblero/pkg/blero). There is also a demo repo [goblero-demo](https://github.com/didil/goblero-demo/tree/master)

````
// Create a new Blero backend
bl := blero.New("db/")

// Start Blero
bl.Start()

// defer Stopping Blero
defer bl.Stop()

// register a processor
bl.RegisterProcessorFunc(func(j *blero.Job) error {
// Do some processing
})

// enqueue a job
bl.EnqueueJob("MyJob", []byte("My Job Data"))

````

## Contributing
All contributions (PR, feedback, bug reports, ideas, etc.) are welcome !

[![](https://sourcerer.io/fame/didil/didil/goblero/images/0)](https://sourcerer.io/fame/didil/didil/goblero/links/0)[![](https://sourcerer.io/fame/didil/didil/goblero/images/1)](https://sourcerer.io/fame/didil/didil/goblero/links/1)[![](https://sourcerer.io/fame/didil/didil/goblero/images/2)](https://sourcerer.io/fame/didil/didil/goblero/links/2)[![](https://sourcerer.io/fame/didil/didil/goblero/images/3)](https://sourcerer.io/fame/didil/didil/goblero/links/3)[![](https://sourcerer.io/fame/didil/didil/goblero/images/4)](https://sourcerer.io/fame/didil/didil/goblero/links/4)[![](https://sourcerer.io/fame/didil/didil/goblero/images/5)](https://sourcerer.io/fame/didil/didil/goblero/links/5)[![](https://sourcerer.io/fame/didil/didil/goblero/images/6)](https://sourcerer.io/fame/didil/didil/goblero/links/6)[![](https://sourcerer.io/fame/didil/didil/goblero/images/7)](https://sourcerer.io/fame/didil/didil/goblero/links/7)