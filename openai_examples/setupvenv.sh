# need to invoke this file from the root of the project, using "source"
python3 -m venv ./venv
source ./venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
# make the module at ../../dumbvector available by adding it to the path
export PYTHONPATH=../../dumbvector:$PYTHONPATH