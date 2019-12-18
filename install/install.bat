#list of packages
pip freeze > sec

#create
conda env create -f sec.yml
conda activate sec
pip install -r sec