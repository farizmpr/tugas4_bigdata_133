# tugas4_bigdata_133


server.py : menginisialisasi server web CherryPy yang menjalankan Flask app.py untuk membuat konteks engine.py berbasis Spark.<br>
engine.py : inti dari sistem dan menyimpan semua perhitungan yang ada.<br>
app.py : sebagai tempat routing.<br>

#### How to run :<br>
Run 'python server.py'<br>
"Recomender Engine is running on localhost port 5432!" will be displayed if the engine is up and running.

#### Accessible URL<br>
The endine runs on localhost (0.0.0.0) on port number 5432.

### REST API
Top Ratings : GET /int:user_id/ratings/top/int:count 
