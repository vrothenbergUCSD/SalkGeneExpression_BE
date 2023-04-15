# geneExpressionDB_BE
Salk gene expression database back end

## Development

From the root directory of the repo activate the environment with the command:

`source env/bin/activate`

The terminal should now look something like

`(env) (base) ➜  geneExpressionDB_BE git:(main)`

Then start the development server locally using the command:

`uvicorn app.main:app --reload`

In the main.py file make sure the development flag is set to True.  Conversely, make sure it is set to False when pushing to production.