from flask import Flask, request, send_file, Response, jsonify
import csv
import math
from datetime import datetime
from collections import defaultdict, Counter
import zipfile
import io
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
CORS(app)

# Hàm phụ trợ từ mã gốc
def standardize_system_name(system):
    system = str(system).upper().strip()
    if '2G' in system or 'GSM' in system: return '2G'
    elif '3G' in system or 'UMTS' in system: return '3G'
    elif '4G' in system or 'LTE' in system: return '4G'
    elif '5G' in system or 'NR' in system: return '5G'
    return system

def standardize_frequency(freq, system):
    freq = str(freq).strip()
    system = standardize_system_name(system)
    if freq == 'n77': return '3800'
    elif freq == '1874' and system == '3G': return '10587'
    cleaned_freq = ''.join(filter(str.isdigit, freq))
    if system == '2G':
        if cleaned_freq in ['900', '1800']: return cleaned_freq
        elif int(cleaned_freq) < 1000: return '900'
        else: return '1800'
    return cleaned_freq

def get_data_layer(data_usage, tech):
    data_usage = float(data_usage)
    tech = standardize_system_name(tech)
    if tech == '2G':
        if data_usage > 30: return 5
        elif data_usage > 20: return 4
        elif data_usage > 5: return 3
        elif data_usage > 1: return 2
        else: return 1
    elif tech == '3G':
        if data_usage > 5: return 5
        elif data_usage > 3: return 4
        elif data_usage > 1: return 3
        elif data_usage > 0.3: return 2
        else: return 1
    elif tech == '4G':
        if data_usage > 500: return 5
        elif data_usage > 200: return 4
        elif data_usage > 50: return 3
        elif data_usage > 10: return 2
        else: return 1
    else:  # 5G
        if data_usage > 10000: return 6
        elif data_usage > 1000: return 5
        elif data_usage > 5000: return 4
        elif data_usage > 200: return 3
        elif data_usage > 50: return 2
        else: return 1

# Logic Coverage KMZ từ mã gốc
def process_cell(cell, sites, site_cell_counts, site_has_ibc):
    PI = math.pi
    COLORS = {
        'L1': 'FF00FF00', 'L2': 'FFFFFF00', 'L3': 'FF00FFFF', 
        'L4': 'FF9314FF', 'L5': 'FF0000FF', 'L6': 'FFCD0000'
    }
    FREQ_CONFIG = {
        '2G': {
            '900': {'radius': 0.0017, 'beamwidth': 10, 'ibc_radius': 0.00015, 'ibc_beamwidth': 360},
            '1800': {'radius': 0.00125, 'beamwidth': 10, 'ibc_radius': 0.00012, 'ibc_beamwidth': 360}
        },
        '3G': {
            '3088': {'radius': 0.00109, 'beamwidth': 55, 'ibc_radius': 0.0006, 'ibc_beamwidth': 360},
            '10562': {'radius': 0.00103, 'beamwidth': 60, 'ibc_radius': 0.0006, 'ibc_beamwidth': 360},
            '10587': {'radius': 0.00097, 'beamwidth': 60, 'ibc_radius': 0.00054, 'ibc_beamwidth': 360},
            '10612': {'radius': 0.00091, 'beamwidth': 60, 'ibc_radius': 0.00048, 'ibc_beamwidth': 360}
        },
        '4G': {
            '25': {'radius': 0.00071, 'beamwidth': 80, 'ibc_radius': 0.00042, 'ibc_beamwidth': 360},
            '50': {'radius': 0.00071, 'beamwidth': 80, 'ibc_radius': 0.00042, 'ibc_beamwidth': 360},
            '1501': {'radius': 0.00065, 'beamwidth': 80, 'ibc_radius': 0.00039, 'ibc_beamwidth': 360},
            '1874': {'radius': 0.00059, 'beamwidth': 80, 'ibc_radius': 0.00036, 'ibc_beamwidth': 360},
            '900': {'radius': 0.00053, 'beamwidth': 90, 'ibc_radius': 0.00033, 'ibc_beamwidth': 360},
            '3150': {'radius': 0.00047, 'beamwidth': 90, 'ibc_radius': 0.0003, 'ibc_beamwidth': 360}
        },
        '5G': {
            '3800': {'radius': 0.0004, 'beamwidth': 100, 'ibc_radius': 0.0005, 'ibc_beamwidth': 360}
        }
    }
    
    site_data = sites[cell['site_id']]
    tech, freq = standardize_system_name(cell['system']), standardize_frequency(cell['frequency'], cell['system'])
    kml_lines = []
    
    try:
        config = FREQ_CONFIG[tech][freq]
        radius = config['ibc_radius'] if cell['type'] == 2 else config['radius']
        beamwidth = config['ibc_beamwidth'] if cell['type'] == 2 else config['beamwidth']
        site_cell_count = site_cell_counts[cell['site_id']]
        if site_cell_count > 3: radius *= 0.7
        elif site_cell_count > 1: radius *= 0.85
        if cell['type'] == 1: radius *= 0.1
        if cell['type'] == 2: radius *= 0.3
        if cell['type'] == 1 and cell['site_id'] in site_has_ibc: radius *= 0.5
    except KeyError:
        radius = 0.00014 if cell['type'] == 2 else 0.0002
        beamwidth = 65 if cell['type'] == 2 else 90
        if cell['type'] == 1: radius *= 0.3
    
    layer = get_data_layer(cell['data_usage'], tech)
    
    kml_lines.append(f'<Placemark>\n<name>{cell["cell_name"]}</name>\n')
    kml_lines.append('<Snippet maxLines="0"></Snippet>\n')
    kml_lines.append('<description><![CDATA[\n<h3>Thông tin Cell</h3>\n<table border="1" cellpadding="3">\n')
    kml_lines.append(f'<tr><td><b>Công nghệ</b></td><td>{tech}</td></tr>\n')
    kml_lines.append(f'<tr><td><b>Tần số</b></td><td>{freq}</td></tr>\n')
    kml_lines.append(f'<tr><td><b>Nhà cung cấp</b></td><td>{cell["vendor"]}</td></tr>\n')
    kml_lines.append(f'<tr><td><b>Hướng anten</b></td><td>{cell["azimuth"]}°</td></tr>\n')
    if tech == '2G':
        kml_lines.append(f'<tr><td><b>Lưu lượng dữ liệu</b></td><td>{cell["data_usage"]:,.2f} Erl</td></tr>\n')
    else:
        kml_lines.append(f'<tr><td><b>Lưu lượng dữ liệu</b></td><td>{cell["data_usage"]:,.2f} GB</td></tr>\n')
    kml_lines.append('</table>\n]]></description>\n')
    kml_lines.append(f'<styleUrl>#Style_L{layer}</styleUrl>\n')
    kml_lines.append('<Polygon>\n<outerBoundaryIs>\n<LinearRing>\n<coordinates>\n')
    
    if cell['type'] == 0:
        kml_lines.append(f'{site_data["lon"]},{site_data["lat"]},0\n')
        azimuth = cell['azimuth']
        half_bw = beamwidth / 2
        steps = 12
        for i in range(steps, -1, -1):
            angle = azimuth - half_bw + (i * beamwidth / steps)
            rad_angle = PI * angle / 180
            kml_lines.append(f'{site_data["lon"] + radius * math.sin(rad_angle)},{site_data["lat"] + radius * math.cos(rad_angle)},0\n')
        kml_lines.append(f'{site_data["lon"]},{site_data["lat"]},0\n')
    else:
        steps = 24
        for i in range(steps + 1):
            angle = 2 * PI * i / steps
            kml_lines.append(f'{site_data["lon"] + radius * math.cos(angle)},{site_data["lat"] + radius * math.sin(angle)},0\n')
    
    kml_lines.append('</coordinates>\n</LinearRing>\n</outerBoundaryIs>\n</Polygon>\n')
    
    if cell['type'] == 2:
        kml_lines.append('<Placemark>\n')
        kml_lines.append(f'<name>{cell["cell_name"]}_beam</name>\n')
        kml_lines.append('<Snippet maxLines="0"></Snippet>\n')
        kml_lines.append('<LineString>\n<coordinates>\n')
        kml_lines.append(f'{site_data["lon"]},{site_data["lat"]},0\n')
        beam_length = radius * 1.2
        beam_angle = cell['azimuth'] * PI / 180
        kml_lines.append(f'{site_data["lon"] + beam_length * math.sin(beam_angle)},{site_data["lat"] + beam_length * math.cos(beam_angle)},0\n')
        kml_lines.append('</coordinates>\n</LineString>\n')
        kml_lines.append(f'<styleUrl>#Style_L{layer}</styleUrl>\n')
        kml_lines.append('</Placemark>\n')
    
    kml_lines.append('</Placemark>\n')
    return ''.join(kml_lines)

def create_coverage_kml(csv_content):
    PI = math.pi
    COLORS = {
        'L1': 'FF00FF00', 'L2': 'FFFFFF00', 'L3': 'FF00FFFF', 
        'L4': 'FF9314FF', 'L5': 'FF0000FF', 'L6': 'FFCD0000',
        'site_L1': 'FF0000FF', 'site_L2': 'FF00FF00', 'site_L3': 'FFFFFF00',
        'site_L4': 'FF00FFFF', 'site_L5': 'FF00FF00', 'site_L6': 'FF9314FF',
        'repeater': 'FFCD0000'
    }
    today = datetime.now().strftime("%Y%m%d_%H%M")
    sites = defaultdict(dict)
    cells = []
    required_columns = {'SITEID', 'LAT', 'LONG', 'CELLNAME', 'CELLID', 'SYS', 
                        'ARFCN/UARFCN/EARFCN/NR-ARFCN', 'AZIMUTH', 'ANT_HEIGHT', 
                        'TILT', 'HBW', 'VBW', 'DATA', 'PLT', 'TYPE'}

    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        if not required_columns.issubset(reader.fieldnames):
            missing = required_columns - set(reader.fieldnames)
            raise ValueError(f"Missing required columns: {missing}")

        processed_lines = 0
        for i, row in enumerate(reader, start=2):
            processed_lines += 1
            try:
                site_id = row['SITEID']
                lat = float(row['LAT'])
                lon = float(row['LONG'])
                cell_name = row['CELLNAME']
                cell_id = row['CELLID']
                system = row['SYS']
                vendor = row.get('VENDOR', 'N/A')
                desc = row.get('DESC', 'N/A')
                freq = row['ARFCN/UARFCN/EARFCN/NR-ARFCN']
                azimuth = float(row['AZIMUTH'])
                height = float(row['ANT_HEIGHT'])
                tilt = float(row['TILT'])
                hbw = float(row['HBW'])
                vbw = float(row['VBW'])
                data_usage = float(row['DATA'])
                plt = int(row['PLT'])
                cell_type = int(row['TYPE'])

                if site_id not in sites:
                    sites[site_id] = {'lat': lat, 'lon': lon, 'desc': desc, 'province': row.get('PROVINCE', 'N/A'),
                                      'vendor': vendor, 'plt': plt}
                
                cells.append({'site_id': site_id, 'cell_name': cell_name, 'cell_id': cell_id, 'system': system,
                              'frequency': freq, 'azimuth': azimuth, 'height': height, 'tilt': tilt,
                              'h_beamwidth': hbw, 'v_beamwidth': vbw, 'data_usage': data_usage,
                              'plt': plt, 'vendor': vendor, 'type': cell_type})
            except ValueError as e:
                continue

    if not sites or not cells:
        raise ValueError("No valid data to create KML.")

    site_cell_counts = Counter(cell['site_id'] for cell in cells)
    site_has_ibc = {cell['site_id'] for cell in cells if cell['type'] == 2}

    kml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n',
        '<Document>\n',
        f'<name>Network_Coverage_{today}</name>\n'
    ]
    
    for layer in ['L1', 'L2', 'L3', 'L4', 'L5', 'L6']:
        kml_lines.append(f'<Style id="Style_{layer}">\n<IconStyle><Icon></Icon></IconStyle>\n')
        kml_lines.append(f'<LabelStyle><color>{COLORS[layer]}</color></LabelStyle>\n')
        kml_lines.append(f'<LineStyle><color>{COLORS[layer]}</color></LineStyle>\n')
        kml_lines.append(f'<PolyStyle><color>b3{COLORS[layer][2:]}</color></PolyStyle>\n</Style>\n')
    
    for i in range(1, 7):
        kml_lines.append(f'<Style id="Style_site_L{i}">\n')
        kml_lines.append(f'<IconStyle><color>{COLORS[f"site_L{i}"]}</color><scale>0.9</scale>\n')
        kml_lines.append('<Icon><href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href></Icon>\n')
        kml_lines.append('</IconStyle>\n')
        kml_lines.append('<LabelStyle><scale>1.0</scale></LabelStyle>\n')
        kml_lines.append('</Style>\n')
    
    kml_lines.append('<Style id="FolderStyleSites">\n<ListStyle>\n</ListStyle>\n<LabelStyle><scale>0</scale></LabelStyle>\n</Style>\n')
    kml_lines.append('<Style id="FolderStyleCells">\n<ListStyle>\n<listItemType>checkHideChildren</listItemType>\n</ListStyle>\n<LabelStyle><scale>0</scale></LabelStyle>\n</Style>\n')

    kml_lines.append('<Folder>\n<name>Sites</name>\n<open>1</open>\n<styleUrl>#FolderStyleSites</styleUrl>\n')
    total_sites = len(sites)
    for i, (site_id, site_data) in enumerate(sites.items(), 1):
        kml_lines.append(f'<Placemark>\n<name>{site_id}</name>\n<Snippet maxLines="0"></Snippet>\n')
        kml_lines.append('<description><![CDATA[\n<h3>Thông tin Site</h3>\n<table border="1" cellpadding="3">\n')
        kml_lines.append(f'<tr><td><b>Tỉnh</b></td><td>{site_data["province"]}</td></tr>\n')
        kml_lines.append(f'<tr><td><b>Loại trạm </b></td><td>{site_data["desc"]}</td></tr>\n')
        kml_lines.append(f'<tr><td><b>Phân Loại Trạm</b></td><td>{site_data["plt"]}</td></tr>\n</table>\n]]></description>\n')
        kml_lines.append(f'<styleUrl>#Style_site_L{min(site_data["plt"], 6)}</styleUrl>\n')
        kml_lines.append(f'<Point>\n<coordinates>{site_data["lon"]},{site_data["lat"]},0</coordinates>\n</Point>\n</Placemark>\n')
    kml_lines.append('</Folder>\n')

    kml_lines.append('<Folder>\n<name>Cells</name>\n<open>0</open>\n<styleUrl>#FolderStyleCells</styleUrl>\n')
    with ThreadPoolExecutor() as executor:
        cell_results = list(executor.map(lambda c: process_cell(c, sites, site_cell_counts, site_has_ibc), cells))
        for result in cell_results:
            kml_lines.append(result)
    kml_lines.append('</Folder>\n')
    kml_lines.append('</Document>\n</kml>\n')
    
    return ''.join(kml_lines)

# Logic Points KMZ từ mã gốc
def process_site(row, sites_set):
    try:
        site_id = row['SITEID']
        lat = float(row['LAT'])
        lon = float(row['LONG'])
        note = row['NOTE']
        if site_id not in sites_set:
            sites_set.add(site_id)
            return {'site_id': site_id, 'lat': lat, 'lon': lon, 'note': note}
    except ValueError:
        return None

def create_points_kml(csv_content, color, size, icon):
    sites = []
    sites_set = set()
    required_columns = {'SITEID', 'LAT', 'LONG', 'NOTE'}

    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        if not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"Missing required columns: {required_columns - set(reader.fieldnames)}")

        with ThreadPoolExecutor() as executor:
            for site in executor.map(lambda row: process_site(row, sites_set), reader):
                if site:
                    sites.append(site)

    if not sites:
        raise ValueError("No valid data to create KML.")

    kml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n',
        '<Document>\n',
        f'<name>Network_Sites_{datetime.now().strftime("%Y%m%d_%H%M")}</name>\n',
        f'<Style id="customStyle">\n<IconStyle>\n<color>{color}</color>\n<scale>{size}</scale>\n',
        f'<Icon><href>http://maps.google.com/mapfiles/kml/shapes/{icon}.png</href></Icon>\n',
        '</IconStyle>\n</Style>\n',
        '<Folder>\n<name>Sites</name>\n<visibility>0</visibility>\n'
    ]

    for site in sites:
        kml_lines.append(f'<Placemark>\n<name>{site["site_id"]}</name>\n')
        kml_lines.append('<Snippet maxLines="0"></Snippet>\n')
        kml_lines.append('<description><![CDATA[\n')
        kml_lines.append(f'<h3>Thông tin Site</h3>\n')
        kml_lines.append(f'<p><b>SiteID:</b> {site["site_id"]}</p>\n')
        kml_lines.append(f'<p><b>Latitude:</b> {site["lat"]}</p>\n')
        kml_lines.append(f'<p><b>Longitude:</b> {site["lon"]}</p>\n')
        kml_lines.append(']]></description>\n')
        kml_lines.append('<styleUrl>#customStyle</styleUrl>\n')
        kml_lines.append(f'<Point>\n<coordinates>{site["lon"]},{site["lat"]},0</coordinates>\n</Point>\n')
        kml_lines.append('</Placemark>\n')

    kml_lines.append('</Folder>\n')
    kml_lines.append('</Document>\n</kml>\n')
    return ''.join(kml_lines)

# Logic Convert CLF từ mã gốc
def convert_csv_to_clf(csv_content):
    required_columns = {'MCCMNC', 'CELLID', 'LAC', 'TYPE', 'LAT', 'LONG', 'POS-RAT', 
                        'DESC', 'SYSCLF', 'CELLNAME', 'AZIMUTH', 'ANT_HEIGHT', 'HBW', 
                        'VBW', 'TILT', 'SITEID'}
    
    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        if not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"Missing required columns: {required_columns - set(reader.fieldnames)}")
        
        clf_lines = []
        for i, row in enumerate(reader, 1):
            try:
                cell_id = row['CELLID']
                if row['SYSCLF'] == '4':
                    try:
                        eNodeB_id, cid = map(int, cell_id.split('-'))
                        cell_id = (eNodeB_id * 256) + cid
                    except (ValueError, AttributeError):
                        pass
                line = (
                    f"{row['MCCMNC']};{cell_id};{row['LAC']};{row['TYPE']};{row['LAT']};{row['LONG']};"
                    f"{row['POS-RAT']};{row['DESC']};{row['SYSCLF']};{row['CELLNAME']};{row['AZIMUTH']};"
                    f"{row['ANT_HEIGHT']};{row['HBW']};{row['VBW']};{row['TILT']};{row['SITEID']}"
                )
                clf_lines.append(line)
            except Exception:
                continue
        
        if not clf_lines:
            raise ValueError("No valid data to convert to CLF.")
        return '\n'.join(clf_lines)

# API Endpoints
@app.route('/coverage-kmz', methods=['POST'])
def coverage_kmz():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        csv_content = file.read().decode('utf-8-sig')
        
        kml_content = create_coverage_kml(csv_content)
        
        output_kmz = io.BytesIO()
        with zipfile.ZipFile(output_kmz.kmz, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("doc.kml", kml_content)
        output_kmz.seek(0)
        
        return send_file(
            output_kmz,
            mimetype='application/vnd.google-earth.kmz',
            download_name=f"Network_Coverage_{datetime.now().strftime('%Y%m%d_%H%M')}*.kmz",
            as_attachment=True
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/points-kmz', methods=['POST'])
def points_kmz():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        csv_content = file.read().decode('utf-8-sig')
        color = request.form.get('color', 'ff00ff00')
        size = request.form.get('size', '1.0')
        icon = request.form.get('icon', 'placemark_circle')
        
        kml_content = create_points_kml(csv_content, color, size, icon)
        
        output_kmz = io.BytesIO()
        with zipfile.ZipFile(output_kmz.kmz, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("doc.kml", kml_content)
        output_kmz.seek(0)
        
        return send_file(
            output_kmz,
            mimetype='application/vnd.google-earth.kmz',
            download_name=f"Network_Sites_{datetime.now().strftime('%Y%m%d_%H%M')}.kmz",
            as_attachment=True
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/convert-clf*.clf', methods=['POST'])
def convert_clf():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        csv_content = file.read().decode('utf-8-sig')
        
        clf_content = convert_csv_to_clf(csv_content)
        
        return send_file(
            io.BytesIO(clf_content.encode('utf-8')),
            mimetype='text/plain',
            download_name=f"Network_Data_{datetime.now().strftime('%Y%m%d_%H%M')}*.clf",
            as_attachment=True
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
