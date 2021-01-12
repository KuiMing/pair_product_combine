import os
import gc
from flask import (
    Flask,
    request,
    redirect,
    url_for,
    send_from_directory,
    render_template,
    current_app,
)
from werkzeug.utils import secure_filename
import pandas as pd
from combine import combine_ohlc

# from io import StringIO

UPLOAD_FOLDER = "."
ALLOWED_EXTENSIONS = set(["csv", "txt", "asc"])

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config['HISTORY'] = "{}/shioaji_history".format(os.getenv("HOME"))
app.config["FILES"] = glob.glob('{}/*.csv'.format(app.config['HISTORY']))

@app.route("/favicon.ico")
def favicon():
    return current_app.send_static_file("favicon.ico")


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1] in ALLOWED_EXTENSIONS


@app.route("/", methods=["GET", "POST"])
def upload_file():
    gc.collect()
    if request.method == "POST":
        # uploaded_files = request.files.getlist("file[]")
        uploaded_files = [request.files.get("file1"), request.files.get("file2")]
        ratios = [int(request.form["ratio" + str(i)]) for i in range(1, 4)]
        intercept = int(request.form["intercept"])
        ops = []
        for i in range(1, 6):
            ops.append(request.form["operator" + str(i)])
        filenames = []
        for file in uploaded_files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                filenames.append(filename)
        new_product, corr = combine_ohlc(filenames, ratios, intercept, ops)
        corr.to_csv("correlation.csv", index=False)
        new_product.to_csv("new_product.csv", index=False)
        return redirect(url_for("report"))

    return render_template("upload_file.html")

@app.route("/search", methods=["GET", "POST"])
def search():
    gc.collect()
    if request.method == "POST":
        code = request.form["code"]
        try:
            file_path = next(
                i
                for i in app.config["FILES"]
                if i == '{0}/{1}.csv'.format(app.config['HISTORY'], code)
            )
            print(file_path)
        except StopIteration:
            print('nothing')
            return "nothing"
        return redirect(url_for("history", code=code))

    return render_template("search.html")

@app.route("/report")
def report():
    base = """
    <!doctype html>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <link rel="stylesheet" href="https://www.w3schools.com/w3css/4/w3.css">
    <link rel="stylesheet" href="static/css/main.css">
    <title>Report</title>
    <div class="bgimg w3-display-container w3-animate-opacity w3-text-white">
    <div class="w3-display-middle">
    """
    button = """
    <a href='/download'><button class="btn"><i class="fa fa-download"></i>New Product</button></a>
    """
    filename = ["correlation.csv", "new_product.csv"]
    corr = pd.read_csv(filename[0])
    new = pd.read_csv(filename[1])
    correlation = corr.to_html(index=False, classes="w3-table w3-bordered w3-border")
    correlation = correlation.replace("thead", "thead class='w3-black'")
    correlation = correlation.replace("tbody", "tbody class='w3-white'")
    distortion = "<br>失真率： {}</br>".format(len(new[new.Open.isna()]) / len(new))
    html = base + correlation + distortion + button + "</div></div>"
    return html


@app.route("/download_file/<filename>")
def download_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


@app.route("/download")
def download():
    print("as_attachment=True")
    return redirect(url_for("download_file", filename="new_product.csv"))

@app.route("/history/<code>")
def history(code):
    return send_from_directory(app.config['HISTORY'], code + ".csv")

if __name__ == "__main__":
    app.run(debug=True)
