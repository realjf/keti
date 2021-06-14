GCC=go
GCMD=run 
GPATH=main.go  

run:
	$(GCC) $(GCMD) $(GPATH)

install:
	go mod tidy


