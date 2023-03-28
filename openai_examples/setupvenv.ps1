python3.9 -m venv ./venv
./venv/Scripts/activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
# make the module at ../../dumbvector available by adding it to the path
$env:PYTHONPATH = "../../dumbvector"