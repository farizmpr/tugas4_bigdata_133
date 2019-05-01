### TUGAS 4 BIG DATA<br>

## recomendation system<br>

A recomendation engine made in python utilizing spark and flask functionality. Based on the recomender engine made by github user "jadianes". This engine implmenents collaborative filtering algorithm like ALS to output recomendation based on it's calculation.

server.py : menginisialisasi server web CherryPy yang menjalankan Flask app.py untuk membuat konteks engine.py berbasis Spark.<br> 
engine.py : inti dari sistem dan menyimpan semua perhitungan yang ada. <br>
app.py : sebagai tempat routing.<br>

How to run :<br>
Run 'python server.py'<br>
"Recomender Engine is running on localhost port 5432!" will be displayed if the engine is up and running.<br>

Accessible URL<br>
The endine runs on localhost (0.0.0.0) on port number 5432.<br>

### REST API<br>
Top Ratings : GET /int:user_id/ratings/top/int:count<br> 

![alt text](https://github.com/farizmpr/tugas4_bigdata_133/blob/master/img/get_awal.png)
