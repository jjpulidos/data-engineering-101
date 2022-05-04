set dotenv-load

setting-venv:
    @echo "Setting up Virtual Environment for Python"
    pip install virtualenv
    virtualenv venv-test
    source venv-test/bin/activate && pip install -r requirements.txt

ingest:
    @echo "Ingesting Data"
    source venv-test/bin/activate && python utils/ingest.py 3

transform-write:
    @echo "Standardizing Schema and storing in DB "
    source venv-test/bin/activate && python utils/write.py


start-pipeline:
    just setting-venv
    just ingest
    just transform-write