"""Lance hack"""
import base64
from io import BytesIO
import json
from pathlib import Path

import pandas as pd
from PIL import Image


def _convert_bytes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for name in df.columns:
        if _is_image_bytes(df[name]):
            df[name] = df[name].apply(_to_base64)
    return df


def _is_image_bytes(ser):
    for k, v in ser.items():
        if pd.isna(v):
            continue
        elif isinstance(v, bytes):
            if len(v) == 0:
                continue
            try:
                Image.open(BytesIO(v))
                return True
            except Exception:
                import pdb; pdb.set_trace()
                return False
        return False


def _to_base64(v):
    if pd.isna(v) or len(v) == 0:
        return None
    else:
        return base64.b64encode(v).decode('UTF-8')


# Imagine the %%sql magic returns an instance of this
class ResultSet:

    def __init__(self, df, user_ns_name):
        self.df = df
        self.user_ns_name = user_ns_name

    def to_json(self, output_file=None):
        df = _convert_bytes(self.df)
        if output_file is None:
            return json.dumps(df.to_dict(orient='records'))
        else:
            return json.dump(df.to_dict(orient='records'), output_file)

    def _repr_html_(self):
        index_html = Path(__file__).parent / 'index.html'
        with open(r'df.json', 'w') as fh:
            self.to_json(fh)
        with index_html.open(mode='r') as fh:
            return fh.read()

    def _gen_html(self):
        return f"""
        <!DOCTYPE html>
        <html>
          <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
              {STYLES}
            </style>
          </head>

          <body>
            {DUMB_GRID}
            <script>
            {self._gen_script()}
            </script>
          </body>
        </html>
        """

    def _gen_script(self):
        # This is like a simplified "/preview API call"
        python_cmd = f"{self.user_ns_name}.to_json()"
        callback = """
        {
            iopub: {
                // this is the response callback
                output: function(response) {
                    // Results come out as plain text
                    var output = response.content.data["text/plain"];

                    // Remove unwanted characters that breaks json parsing
                    output = output.substring(1, output.length-1).replace("\\'","'");

                    var rows = JSON.parse(output);

                    // Simple hack here just for illustration
                    var i = 0;
                    var columns = document.getElementsByClassName("column");
                    for (c of columns) {
                        var images = c.getElementsByTagName("img");  
                        for (img of images) {
                           img.setAttribute("src", rows[i]["external_image"]); 
                           i++; 
                        };
                    };
                }
            }
        }
        """

        config = """
        {
            silent: false,
            store_history: false,
            stop_on_error: true
        }
        """

        return f"""
        IPython.notebook.kernel.execute("{python_cmd}", {callback}, {config});
        {GRID_JS}
        """


STYLES = """
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: Arial, Helvetica, sans-serif;
}

.header {
  text-align: center;
  padding: 32px;
}

.row {
  display: -ms-flexbox; /* IE 10 */
  display: flex;
  -ms-flex-wrap: wrap; /* IE 10 */
  flex-wrap: wrap;
  padding: 0 4px;
}

/* Create two equal columns that sits next to each other */
.column {
  -ms-flex: 50%; /* IE 10 */
  flex: 50%;
  padding: 0 4px;
}

.column img {
  margin-top: 8px;
  vertical-align: middle;
}

/* Style the buttons */
.btn {
  border: none;
  outline: none;
  padding: 10px 16px;
  background-color: #f1f1f1;
  cursor: pointer;
  font-size: 18px;
}

.btn:hover {
  background-color: #ddd;
}

.btn.active {
  background-color: #666;
  color: white;
}
"""

DUMB_GRID="""
<!-- Header -->
        <div class="header" id="myHeader">
          <h1>Image Grid</h1>
          <p>Click on the buttons to change the grid view.</p>
          <button class="btn" onclick="one()">1</button>
          <button class="btn active" onclick="two()">2</button>
          <button class="btn" onclick="four()">4</button>
        </div>

        <!-- Photo Grid -->
        <div class="row"> 
          <div class="column">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
          </div>
          <div class="column">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
          </div>  
          <div class="column">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
          </div>
          <div class="column">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
            <img src="" style="width:100%">
          </div>
        </div>
"""

GRID_JS = """

// Get the elements with class="column"
var elements = document.getElementsByClassName("column");

// Declare a loop variable
var i;

// Full-width images
function one() {
    for (i = 0; i < elements.length; i++) {
    elements[i].style.msFlex = "100%";  // IE10
    elements[i].style.flex = "100%";
  }
}

// Two images side by side
function two() {
  for (i = 0; i < elements.length; i++) {
    elements[i].style.msFlex = "50%";  // IE10
    elements[i].style.flex = "50%";
  }
}

// Four images side by side
function four() {
  for (i = 0; i < elements.length; i++) {
    elements[i].style.msFlex = "25%";  // IE10
    elements[i].style.flex = "25%";
  }
}

// Add active class to the current button (highlight it)
var header = document.getElementById("myHeader");
var btns = header.getElementsByClassName("btn");
for (var i = 0; i < btns.length; i++) {
  btns[i].addEventListener("click", function() {
    var current = document.getElementsByClassName("active");
    current[0].className = current[0].className.replace(" active", "");
    this.className += " active";
  });
}
"""

