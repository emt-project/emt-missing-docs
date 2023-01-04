import glob
import jinja2
import pandas as pd
import os
import shutil
from acdh_tei_pyutils.tei import TeiReader
from dateutil.parser import parse, ParserError
from datetime import date
import tqdm
import lxml.etree as ET
from collections import defaultdict

editions = "./data/editions"
shutil.rmtree(editions)
os.makedirs(editions, exist_ok=True)

files = sorted(glob.glob('./trans_out/*.xml'))
facs = {}
for x in files:
    doc = TeiReader(x)
    nsmap = doc.nsmap
    pbs = doc.any_xpath('.//tei:pb')
    d = defaultdict(list)
    for ab in doc.any_xpath('.//tei:ab'):
        facs_id = ab.attrib['facs']
        page_id = "_".join(facs_id.split('_')[:2])
        d[page_id].append(ET.tostring(ab, encoding='utf-8', pretty_print=True).decode('utf-8'))
    for i, p in enumerate(doc.any_xpath('.//tei:surface')):
        img_id = p.xpath("./tei:graphic[1]/@url", namespaces=nsmap)[0]
        facs_id = p.attrib["{http://www.w3.org/XML/1998/namespace}id"]
        pb_node = ET.tostring(pbs[i], encoding='utf-8', pretty_print=True).decode('utf-8')
        ab_nodes = "".join(d.get(f"#{facs_id}"))
        page = pb_node + ab_nodes
        facs[img_id] = {
            "img_file_name": img_id,
            "surface_xmlid": facs_id,
            "surface_node": ET.tostring(p, encoding='utf-8', pretty_print=True).decode('utf-8').replace(' xmlns="http://www.tei-c.org/ns/1.0"', ''),
            "page": page.replace(' xmlns="http://www.tei-c.org/ns/1.0"', '')
        }

templateLoader = jinja2.FileSystemLoader(searchpath="./scripts/templates")
templateEnv = jinja2.Environment(loader=templateLoader)
template = templateEnv.get_template('tei_template.xml')

df = pd.read_csv('gesamtliste_enriched.csv')

for gr, ndf in tqdm.tqdm(df.groupby('folder')):
    file_name = f"./data/editions/{gr}.xml"
    if '44_8_' in gr or '45_8_' in gr:
        payload = []
        for i, nrow in ndf.iterrows():
            img_name = nrow['Dateiname']
            try:
                payload.append(facs[img_name])
            except KeyError:
                continue
        if len(payload) < 1:
            continue
        body = ""
        facs_nodes = ""
        for x in payload:
            body = body + x['page']
            facs_nodes = facs_nodes + x['surface_node']
    else:
        continue
    with open(file_name.lower(), 'w') as f:
        row = ndf.iloc[0]
        
        item = {}
        if '_blau_44' in gr:
            doc_id = 565905
        else:
            doc_id = 402711
        item['doc_id'] = doc_id
        item['col_id'] = "58705"
        item['settlement'] = "München"
        item['repositor'] = "Bayerisches Hauptstaatsarchiv"
        item['id'] = gr.lower()
        item['file_name'] = f"{gr}.xml".lower()
        item['title'] = f"{row['weranwen']}, {row['Ort']} am {row['Datum']}"
        qka = row['Quellenkritische Anmerkungen']
        item["terms"] = []
        try:
            for x in qka.split(';')[1:]:
                item["terms"].append(x.strip())
        except:
            pass
        try:
            item["language"] = qka.split(';')[0].strip()
        except AttributeError:
            item["language"] = "noch nicht bestimmt"
        if item["language"].startswith("deu"):
            item["lang_code"] = "deu"
        elif item["language"].startswith("lat"):
            item["lang_code"] = "lat"
        elif item["language"].startswith("franz"):
            item["lang_code"] = "fra"
        elif item["language"].startswith("ital"):
            item["lang_code"] = "ita"
        else:
            item["lang_code"] = "und"
        try:
            item['sender'] = row['weranwen'].split(' an ')[0]
        except:
            item['sender'] = row['weranwen']
        try:
            if item["sender"].startswith("Eleonora"):
                item["sender_id"] = "emt_person_id__9"
            elif item["sender"].startswith("Philipp Wilhelm von Pfalz-Neuburg"):
                item["sender_id"] = "emt_person_id__50"
            elif item["sender"].startswith("Johann Wilhelm von Pfalz"):
                item["sender_id"] = "emt_person_id__18"
        except:
            pass
        try:
            item['receiver'] = row['weranwen'].split(' an ')[-1]
        except:
            item['receiver'] = row['weranwen']
        try:
            if item["receiver"].startswith("Eleonora"):
                item["receiver_id"] = "emt_person_id__9"
            elif item["receiver"].startswith("Philipp Wilhelm von Pfalz-Neuburg"):
                item["receiver_id"] = "emt_person_id__50"
            elif item["receiver"].startswith("Johann Wilhelm von Pfalz"):
                item["receiver_id"] = "emt_person_id__18"
        except:
            pass
        try:
            row['Ort'].strip()
            item["sender_place"] = row['Ort'].strip()
            if item["sender_place"].startswith('Wien'):
                item["sender_id_place"] = "emt_place_id__63"
            elif item["sender_place"].startswith('Düsseldorf'):
                item["sender_id_place"] = "emt_place_id__13"
            elif item["sender_place"].startswith('Laxenburg'):
                item["sender_id_place"] = "emt_place_id__31"
            elif item["sender_place"].startswith('Linz'):
                item["sender_id_place"] = "emt_place_id__32"
            elif item["sender_place"].startswith('Neuburg'):
                item["sender_id_place"] = "emt_place_id__39"
            elif item["sender_place"].startswith('Bensberg'):
                item["sender_id_place"] = "emt_place_id__7"
        except:
            pass
        try:
            item["bemerkung"] = row["Bemerkung"].strip()
        except:
            pass
        item['place'] = row['Ort']
        item['writte_date'] = row['Datum']
        item['current_date'] = f"{date.today()}"
        item["facsimile"] = facs_nodes
        body_string = body
        body_string = body_string.replace('reason=""', '')
        body_string = body_string.replace('type=""', '')
        body_string = body_string.replace('<blackening>', '<seg type="blackening">')
        body_string = body_string.replace('</blackening>', '</seg>')
        body_string = body_string.replace('<comment>', '<seg type="comment">')
        body_string = body_string.replace('</comment>', '</seg>')
        body_string = body_string.replace('<blackening/>', '')
        body_string = body_string.replace('<comment/>', '')
        item["body_string"] = body_string
        try:
            item['parsed_date'] = parse(item['writte_date'])
        except (ParserError, TypeError):
            item['parsed_date'] = None
        item['pages'] = []
        f.write(template.render(**item))

files = sorted(glob.glob(f"{editions}/*.xml"))
print(len(files))

for x in files:
    doc = TeiReader(x)
    for bad in doc.any_xpath('.//tei:surface/tei:graphic[2]'):
        bad.getparent().remove(bad)
    doc.tree_to_file(x)

print("fix file names")
df = pd.read_csv('gesamtliste_enriched.csv')
no_match = []
page_count_issue = []
for g, ndf in df.groupby('folder'):
    xml_doc = f"{editions}/{g.lower()}.xml"
    pages = []
    try:
        doc = TeiReader(f"{xml_doc}")
    except OSError:
        no_match.append(xml_doc)
        continue
    for i, row in ndf.iterrows():
        page = {
            "file__name": row['Dateiname'],
            "fol_1": row['Foliierung'],
            "fol_2": row['Zweitfoliierung']
        }
        pages.append(page)
    for i, pb in enumerate(doc.any_xpath('.//tei:pb')):
        try:
            matching_page = pages[i]
        except IndexError:
            issue = {
                "doc": xml_doc,
                "page_nr": i
            }
            continue
        if isinstance(pages[i]['fol_1'], str):
            pb.attrib["n"] = pages[i]['fol_1']
        if isinstance(pages[i]['fol_2'], str):
            pb.attrib["ed"] = pages[i]['fol_2']
        pb.attrib["{http://www.w3.org/XML/1998/namespace}id"] = pages[i]['file__name']
    for i, graphic in enumerate(doc.any_xpath('.//tei:surface/tei:graphic[1]')):
        try:
            matching_page = pages[i]
        except IndexError:
            issue = {
                "doc": xml_doc,
                "page_nr": i
            }
            continue
        graphic.attrib["url"] = pages[i]['file__name']
    doc.tree_to_file(xml_doc)
print(page_count_issue)