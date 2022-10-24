cd %userprofile%\Downloads

:: install GIT
curl https://github.com/git-for-windows/git/releases/download/v2.38.1.windows.1/Git-2.38.1-64-bit.exe -LO
start /WAIT Git-2.38.1-64-bit.exe

:: install Python
curl https://www.python.org/ftp/python/3.10.8/python-3.10.8-amd64.exe -LO
start /WAIT python-3.10.8-amd64.exe

:: install all necessary packages
python -m pip install pyyaml
python -m pip install sshtunnel
python -m pip install psycopg2
python -m pip install sqlalchemy
python -m pip install redshift_connector
python -m pip install pandas