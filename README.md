### TUGAS 4 BIG DATA<br>
server.py : menginisialisasi server web CherryPy yang menjalankan Flask app.py untuk membuat konteks engine.py berbasis Spark.<br> 
engine.py : inti dari sistem dan menyimpan semua perhitungan yang ada. <br>
app.py : penghubung server.py dan engine.py, sebagai tempat routing.<br>

How to run :<br>
Run 'python server.py'<br>
"Recomender Engine is running on localhost port 5432!" will be displayed if the engine is up and running.<br>

Accessible URL<br>
The endine runs on localhost (0.0.0.0) on port number 5432.<br>

### REST API<br>
Top Ratings : GET /int:user_id/ratings/top/int:count<br> 
