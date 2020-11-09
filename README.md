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
	- db connection.
	- db disconnection.
	- method to obtain table rows.

### DL Class
	- dl connection.
	- dl disconnection.
	- file system operation.
	- method to send to dl.
	- method to modify file structure.

### CDC Class

	- db,dl connection.
	- send data to dl in a consistency way (transaction).
	- change capture method(CDC).
	- method to set time.

---

### To Do

- Add documentation to all function
- Add to `send_to_dl` function a sign to verify if sent data are correct
- Add test suite
	- test Registry Data
	- test Log Data
	- test Transaction on `send_to_dl`
	- test INSERT, UPDATE, REMOVE
- Refactor all code
- Resolve bug on UPDATE

---

### Important Note
To execute succesfully you have to create a directory named **tmp_dl** and put it in the *src* directory.
