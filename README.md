# challengeBigDataCDC

- I want you to architect and develop (the latter is mandatory only for groups) an incremental batch job able to move data from a source to a data lake using the CDC pattern we have seen. The architecture must allow developer to implement specific version of the CDC on different sources/with different CDC logics/... .

- I've asked to architect and develop the solution. Architect means you are writing code keeping in mind someone is going to develop over your architecture. To success on the challenge, you must immagine two different people is going to do the two different phase:
  - Architect: design the solution, with abstract methodes and concrete one
  - Developer: take the artchitecture and using it, implement a solution for a specific (and real) case

- I don't expect (except for very large group) a end to end working solution:
  - Decide most important methods and develop them
  - For other, use a stub (https://en.wikipedia.org/wiki/Method_stub)

- The core part of the challenge is the architectural one: the incremental algorithm is less important than the full picture

So, try to be in the architect shoes and architect your first solution: this is stricly related with all concepts we have seen before the Hadoop lesson, so multiple point of view, various pattern, architectural principle, and so far so on.
The maximum point achievable is +2, but this time I'll start to given part of the total score only to completely correct solution.

---

## Solution

### DB class
	- connessione db
	- disconnessione db
	- Get method che restituisce tutte le righe del db

### DL Class
	- connessione db
	- disconnessione db
	- file system operation
	- send to dl method
	- modify file structure method

### CDC Class

	- connessione a DL e DB
	- send data to dl in a consistency way (transaction)
	- change capture method
	- timing set method

---

## To Do

- Add documentation to all function
- Add to `send_to_dl` function a sign to verify if sent data are correct
- Add test suite
	- test Registry Data
	- test Log Data
	- test Transaction on `send_to_dl`
	- test INSERT, UPDATE, REMOVE
- Refactor all code