set here=%~dp0
set input_dir=%1
set output_dir=%2
set script=%here%\convert.py

python %script% %input_dir% %output_dir%

