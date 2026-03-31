from qgis.PyQt.QtCore import QMetaType
from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterString,
    QgsProcessingParameterFileDestination,
    QgsProcessingParameterNumber,
    QgsProcessingParameterFile,
    QgsProcessingException,
    QgsFeature,
    QgsField,
    QgsFields,
    QgsVectorLayer,
    QgsVectorFileWriter,
    QgsProject,
    QgsCategorizedSymbolRenderer,
    QgsRendererCategory,
    QgsSymbol,
)

import os
import csv
import io
import re
import zipfile
import tempfile
import urllib.request
from collections import defaultdict, Counter


class AnncsuAggiudicatariAlgorithm(QgsProcessingAlgorithm):
    PARAM_COMUNI = "COMUNI"
    PARAM_URL_CAND = "URL_CAND"
    PARAM_CUP = "CUP"
    PARAM_AGG = "AGG"
    PARAM_TOPN = "TOPN"
    PARAM_OUT_GPKG = "OUT_GPKG"
    PARAM_OUT_LAYER_NAME = "OUT_LAYER_NAME"
    PARAM_OUT_TOP_NAME = "OUT_TOP_NAME"

    def tr(self, string):
        return string

    def createInstance(self):
        return AnncsuAggiudicatariAlgorithm()

    def name(self):
        return "anncsu_aggiudicatari_processing"

    def displayName(self):
        return "ANNCSU aggiudicatari comuni"

    def group(self):
        return "Custom scripts"

    def groupId(self):
        return "customscripts"

    def shortHelpString(self):
        return (
            "Scarica/legge candidature 1.3.1, filtra ANNCSU, collega CUP->CIG->aggiudicatari, "
            "fa il join coi comuni via codice ISTAT, crea layer finale e Top N operatori."
        )

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.PARAM_COMUNI,
                "Layer comuni",
                [QgsProcessing.TypeVectorPolygon]
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PARAM_URL_CAND,
                "URL candidature finanziate 131",
                defaultValue="https://raw.githubusercontent.com/teamdigitale/padigitale2026-opendata/main/data/candidature_finanziate_131.csv"
            )
        )

        self.addParameter(
            QgsProcessingParameterFile(
                self.PARAM_CUP,
                "CUP CSV o ZIP",
                behavior=QgsProcessingParameterFile.File,
                fileFilter="CSV (*.csv);;ZIP (*.zip)"
            )
        )

        self.addParameter(
            QgsProcessingParameterFile(
                self.PARAM_AGG,
                "Aggiudicatari CSV",
                behavior=QgsProcessingParameterFile.File,
                fileFilter="CSV (*.csv)"
            )
        )

        self.addParameter(
            QgsProcessingParameterNumber(
                self.PARAM_TOPN,
                "Top N operatori",
                type=QgsProcessingParameterNumber.Integer,
                defaultValue=20,
                minValue=1
            )
        )

        self.addParameter(
            QgsProcessingParameterFileDestination(
                self.PARAM_OUT_GPKG,
                "Output GeoPackage",
                fileFilter="GeoPackage (*.gpkg)"
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PARAM_OUT_LAYER_NAME,
                "Nome layer finale",
                defaultValue="comuni_anncsu_aggiudicatari"
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PARAM_OUT_TOP_NAME,
                "Nome tabella top",
                defaultValue="top_operatori_anncsu"
            )
        )

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def safe_str(self, v):
        return "" if v is None else str(v).strip()

    def norm_text(self, s):
        s = self.safe_str(s).upper()
        s = " ".join(s.split())
        return s

    def normalize_code(self, v):
        s = self.safe_str(v)
        if s.endswith(".0"):
            s = s[:-2]
        s = "".join(ch for ch in s if ch.isdigit())
        if s:
            s = s.zfill(6)
        return s

    def parse_number(self, v):
        if v is None:
            return 0.0

        if isinstance(v, (int, float)):
            return float(v)

        s = str(v)
        s = s.replace("\xa0", " ").replace("\u202f", " ").strip()
        s = s.replace("€", "").replace('"', "").replace("'", "").strip()
        s = re.sub(r"[^0-9,.\-]", "", s)

        if not s:
            return 0.0

        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "")
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(".", "")
            s = s.replace(",", ".")

        try:
            return float(s)
        except Exception:
            return 0.0

    def is_url(self, s):
        s = self.safe_str(s).lower()
        return s.startswith("http://") or s.startswith("https://")

    def download_to_file(self, url, out_path, feedback):
        feedback.pushInfo(f"Download: {url}")
        with urllib.request.urlopen(url) as r, open(out_path, "wb") as f:
            f.write(r.read())
        return out_path

    def open_text_any(self, path_or_url, work_dir, feedback, preferred_member_contains=None):
        if self.is_url(path_or_url):
            filename = os.path.basename(path_or_url.split("?")[0]) or "downloaded_file"
            tmp_path = os.path.join(work_dir, filename)
            self.download_to_file(path_or_url, tmp_path, feedback)
            src = tmp_path
        else:
            src = path_or_url

        if not os.path.exists(src):
            raise QgsProcessingException(f"File non trovato: {src}")

        if src.lower().endswith(".zip"):
            zf = zipfile.ZipFile(src, "r")
            members = zf.namelist()
            csv_members = [m for m in members if m.lower().endswith(".csv")]
            if not csv_members:
                raise QgsProcessingException(f"Nessun CSV trovato nello ZIP: {src}")

            chosen = None
            if preferred_member_contains:
                for m in csv_members:
                    if preferred_member_contains.lower() in m.lower():
                        chosen = m
                        break
            if chosen is None:
                chosen = csv_members[0]

            raw = zf.read(chosen)
            return io.StringIO(raw.decode("utf-8-sig", errors="replace"))

        with open(src, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
            return io.StringIO(f.read())

    def csv_reader_from_any(self, path_or_url, work_dir, feedback, preferred_member_contains=None):
        stream = self.open_text_any(
            path_or_url,
            work_dir,
            feedback,
            preferred_member_contains=preferred_member_contains
        )

        text = stream.read()
        stream.close()

        sample = text[:10000]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
            delimiter = dialect.delimiter
        except Exception:
            delimiter = ";" if sample.count(";") > sample.count(",") else ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

        if reader.fieldnames:
            reader.fieldnames = [
                self.safe_str(f).strip().strip('"').strip("'")
                for f in reader.fieldnames
            ]

        return reader

    def detect_field(self, fieldnames, candidates, dataset_name):
        cleaned = [self.safe_str(f).strip().strip('"').strip("'") for f in fieldnames]
        lower_map = {f.lower(): f for f in cleaned}

        for c in candidates:
            if c.lower() in lower_map:
                return lower_map[c.lower()]

        raise QgsProcessingException(
            f"Nel dataset '{dataset_name}' non trovo nessuno di questi campi: {', '.join(candidates)}. "
            f"Campi disponibili: {', '.join(cleaned)}"
        )

    def choose_first_existing(self, fieldnames, candidates):
        cleaned = [self.safe_str(f).strip().strip('"').strip("'") for f in fieldnames]
        lower_map = {f.lower(): f for f in cleaned}
        for c in candidates:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return None

    def clone_fields(self, src_layer):
        fields = QgsFields()
        for f in src_layer.fields():
            fields.append(QgsField(f.name(), f.type(), f.typeName(), f.length(), f.precision()))
        return fields

    def add_field_if_absent(self, fields_obj, name, qtype=QMetaType.Type.QString, length=254, precision=0):
        if fields_obj.indexOf(name) == -1:
            type_name = ""
            if qtype == QMetaType.Type.QString:
                type_name = "string"
            elif qtype == QMetaType.Type.Int:
                type_name = "integer"
            elif qtype == QMetaType.Type.Double:
                type_name = "double"

            fields_obj.append(QgsField(name, qtype, type_name, length, precision))

    def get_common_code_field(self, layer):
        for cand in ["PRO_COM_T", "pro_com_t", "CODICE_COMUNE", "codice_comune", "ISTAT", "istat"]:
            idx = layer.fields().indexOf(cand)
            if idx != -1:
                return cand
        raise QgsProcessingException(
            "Nel layer comuni non trovo il campo PRO_COM_T/codice ISTAT comune."
        )

    def distinct_color(self, i):
        palette = [
            "#1f78b4", "#33a02c", "#e31a1c", "#ff7f00", "#6a3d9a",
            "#b15928", "#a6cee3", "#b2df8a", "#fb9a99", "#fdbf6f",
            "#cab2d6", "#ffff99", "#8dd3c7", "#80b1d3", "#bebada",
            "#fb8072", "#fdb462", "#b3de69", "#fccde5", "#bc80bd"
        ]
        return QColor(palette[i % len(palette)])

    # ------------------------------------------------------------
    # Main
    # ------------------------------------------------------------

    def processAlgorithm(self, parameters, context, feedback):
        comuni = self.parameterAsVectorLayer(parameters, self.PARAM_COMUNI, context)
        url_cand = self.parameterAsString(parameters, self.PARAM_URL_CAND, context)
        cup_csv_o_zip = self.parameterAsString(parameters, self.PARAM_CUP, context)
        aggiudicatari_csv = self.parameterAsString(parameters, self.PARAM_AGG, context)
        top_n = self.parameterAsInt(parameters, self.PARAM_TOPN, context)
        output_gpkg = self.parameterAsFileOutput(parameters, self.PARAM_OUT_GPKG, context)
        output_layer_name = self.parameterAsString(parameters, self.PARAM_OUT_LAYER_NAME, context)
        output_top_name = self.parameterAsString(parameters, self.PARAM_OUT_TOP_NAME, context)

        if comuni is None:
            raise QgsProcessingException("Layer comuni non valido.")

        if not output_gpkg.lower().endswith(".gpkg"):
            output_gpkg += ".gpkg"

        work_dir = os.path.dirname(output_gpkg)
        if not work_dir:
            work_dir = tempfile.mkdtemp(prefix="anncsu_processing_")
        else:
            os.makedirs(work_dir, exist_ok=True)

        feedback.pushInfo("=== Avvio elaborazione ===")
        feedback.pushInfo(f"Cartella di lavoro: {work_dir}")

        # ------------------------------------------------------------
        # 1. Candidature
        # ------------------------------------------------------------

        cand_local = os.path.join(work_dir, "candidature_finanziate_131.csv")
        self.download_to_file(url_cand, cand_local, feedback)

        cand_reader = self.csv_reader_from_any(cand_local, work_dir, feedback)
        cand_fields = cand_reader.fieldnames or []

        cand_codice_comune = self.detect_field(cand_fields, ["cod_comune"], "candidature")
        cand_avviso = self.detect_field(cand_fields, ["avviso"], "candidature")
        cand_cup = self.detect_field(cand_fields, ["codice_cup", "cup"], "candidature")
        cand_importo = self.detect_field(cand_fields, ["importo_finanziamento"], "candidature")

        comune_to_cup = {}
        comune_to_importo = defaultdict(float)

        rows_total = 0
        rows_anncsu = 0

        feedback.pushInfo("Lettura candidature ANNCSU...")

        for row in cand_reader:
            rows_total += 1
            avviso = self.norm_text(row.get(cand_avviso))
            if "ANNCSU" not in avviso:
                continue

            rows_anncsu += 1
            codice = self.normalize_code(row.get(cand_codice_comune))
            cup = self.norm_text(row.get(cand_cup))
            importo = self.parse_number(row.get(cand_importo))

            if not codice or not cup:
                continue

            if codice not in comune_to_cup:
                comune_to_cup[codice] = cup

            comune_to_importo[codice] += importo

        feedback.pushInfo(f"Candidature lette: {rows_total}")
        feedback.pushInfo(f"Candidature ANNCSU: {rows_anncsu}")
        feedback.pushInfo(f"Comuni ANNCSU con CUP: {len(comune_to_cup)}")

        # ------------------------------------------------------------
        # 2. CUP -> CIG
        # ------------------------------------------------------------

        cup_reader = self.csv_reader_from_any(cup_csv_o_zip, work_dir, feedback, preferred_member_contains="cup")
        cup_fields = cup_reader.fieldnames or []

        cup_field = self.detect_field(cup_fields, ["cup", "codice_cup"], "cup")
        cig_field_in_cup = self.detect_field(cup_fields, ["cig", "codice_cig"], "cup")

        wanted_cups = set(comune_to_cup.values())
        cup_to_cig = {}

        feedback.pushInfo("Indicizzazione CUP -> CIG...")

        for row in cup_reader:
            cup = self.norm_text(row.get(cup_field))
            if cup not in wanted_cups:
                continue

            cig = self.norm_text(row.get(cig_field_in_cup))
            if cig and cup not in cup_to_cig:
                cup_to_cig[cup] = cig

        feedback.pushInfo(f"CUP richiesti: {len(wanted_cups)}")
        feedback.pushInfo(f"CUP con CIG trovato: {len(cup_to_cig)}")

        # ------------------------------------------------------------
        # 3. CIG -> aggiudicatario
        # ------------------------------------------------------------

        agg_reader = self.csv_reader_from_any(aggiudicatari_csv, work_dir, feedback, preferred_member_contains="aggiudicatari")
        agg_fields = agg_reader.fieldnames or []

        agg_cig = self.detect_field(agg_fields, ["cig", "codice_cig"], "aggiudicatari")
        agg_den = self.detect_field(agg_fields, ["denominazione", "ragione_sociale", "aggiudicatario"], "aggiudicatari")
        agg_ruolo = self.choose_first_existing(agg_fields, ["ruolo"])

        wanted_cigs = set(cup_to_cig.values())
        agg_by_cig = {}
        multi_count = Counter()

        feedback.pushInfo("Filtraggio aggiudicatari per CIG...")

        def role_rank(row):
            if not agg_ruolo:
                return 99
            r = self.norm_text(row.get(agg_ruolo))
            if "MANDATAR" in r or "CAPOGRUPPO" in r:
                return 0
            if "MANDANTE" in r:
                return 1
            return 50

        for row in agg_reader:
            cig = self.norm_text(row.get(agg_cig))
            if cig not in wanted_cigs:
                continue

            if cig not in agg_by_cig:
                agg_by_cig[cig] = row
            else:
                multi_count[cig] += 1
                if role_rank(row) < role_rank(agg_by_cig[cig]):
                    agg_by_cig[cig] = row

        feedback.pushInfo(f"CIG richiesti: {len(wanted_cigs)}")
        feedback.pushInfo(f"CIG con aggiudicatario: {len(agg_by_cig)}")

        # ------------------------------------------------------------
        # 4. Join comuni
        # ------------------------------------------------------------

        pro_com_t = self.get_common_code_field(comuni)
        comuni_fields = self.clone_fields(comuni)

        for extra in [
            ("istat_match", QMetaType.Type.QString, 20, 0),
            ("cup_match", QMetaType.Type.QString, 64, 0),
            ("cig_match", QMetaType.Type.QString, 64, 0),
            ("match_status", QMetaType.Type.QString, 32, 0),
            ("fin_comune", QMetaType.Type.Double, 20, 2),
            ("top20_agg", QMetaType.Type.QString, 254, 0),
        ]:
            self.add_field_if_absent(comuni_fields, extra[0], extra[1], extra[2], extra[3])

        for f in agg_fields:
            self.add_field_if_absent(comuni_fields, f"agg_{f}", QMetaType.Type.QString, 254, 0)

        mem_uri = f"Polygon?crs={comuni.crs().authid()}"
        out_layer = QgsVectorLayer(mem_uri, output_layer_name, "memory")
        out_pr = out_layer.dataProvider()
        out_pr.addAttributes(comuni_fields)
        out_layer.updateFields()

        status_counts = Counter()
        operator_counter = Counter()
        operator_fund = defaultdict(float)

        feedback.pushInfo("Costruzione layer finale comuni...")

        new_feats = []

        for ft in comuni.getFeatures():
            codice = self.normalize_code(ft[pro_com_t])

            nf = QgsFeature(out_layer.fields())
            nf.setGeometry(ft.geometry())

            for f in comuni.fields():
                nf[f.name()] = ft[f.name()]

            nf["istat_match"] = codice
            nf["cup_match"] = None
            nf["cig_match"] = None
            nf["match_status"] = "NESSUN_MATCH"
            nf["fin_comune"] = float(comune_to_importo.get(codice, 0.0))
            nf["top20_agg"] = None

            cup = comune_to_cup.get(codice)
            if not cup:
                nf["match_status"] = "NO_CUP"
                status_counts["NO_CUP"] += 1
                new_feats.append(nf)
                continue

            nf["cup_match"] = cup

            cig = cup_to_cig.get(cup)
            if not cig:
                nf["match_status"] = "NO_CIG"
                status_counts["NO_CIG"] += 1
                new_feats.append(nf)
                continue

            nf["cig_match"] = cig

            agg_row = agg_by_cig.get(cig)
            if not agg_row:
                nf["match_status"] = "NO_AGGIUD"
                status_counts["NO_AGGIUD"] += 1
                new_feats.append(nf)
                continue

            for f in agg_fields:
                nf[f"agg_{f}"] = self.safe_str(agg_row.get(f))

            if cig in multi_count:
                nf["match_status"] = "MULTI"
                status_counts["MULTI"] += 1
            else:
                nf["match_status"] = "OK"
                status_counts["OK"] += 1

            operatore = self.norm_text(agg_row.get(agg_den))
            if operatore:
                operator_counter[operatore] += 1
                operator_fund[operatore] += comune_to_importo.get(codice, 0.0)

            new_feats.append(nf)

        out_pr.addFeatures(new_feats)
        out_layer.updateExtents()

        # ------------------------------------------------------------
        # 5. Top N
        # ------------------------------------------------------------

        ranking = sorted(
            operator_counter.keys(),
            key=lambda op: (-operator_counter[op], -operator_fund[op], op)
        )
        top_ops = ranking[:top_n]
        top_set = set(top_ops)

        out_layer.startEditing()
        idx_top = out_layer.fields().indexOf("top20_agg")
        idx_den = out_layer.fields().indexOf(f"agg_{agg_den}")

        for ft in out_layer.getFeatures():
            den = self.norm_text(ft[idx_den]) if idx_den != -1 else ""
            status = self.safe_str(ft["match_status"])

            if status in ("NO_CUP", "NO_CIG", "NO_AGGIUD", "NESSUN_MATCH") or not den:
                value = "NESSUN_MATCH"
            elif den in top_set:
                value = den
            else:
                value = "ALTRO"

            out_layer.changeAttributeValue(ft.id(), idx_top, value)

        out_layer.commitChanges()

        # ------------------------------------------------------------
        # 6. Tabella top
        # ------------------------------------------------------------

        top_tbl = QgsVectorLayer("None", output_top_name, "memory")
        top_pr = top_tbl.dataProvider()
        top_pr.addAttributes([
            QgsField("rank", QMetaType.Type.Int, "integer", 10, 0),
            QgsField("operatore", QMetaType.Type.QString, "string", 254, 0),
            QgsField("n_comuni", QMetaType.Type.Int, "integer", 10, 0),
            QgsField("somma_fin", QMetaType.Type.Double, "double", 20, 2),
        ])
        top_tbl.updateFields()

        top_feats = []
        for i, op in enumerate(top_ops, start=1):
            f = QgsFeature(top_tbl.fields())
            f["rank"] = i
            f["operatore"] = op
            f["n_comuni"] = int(operator_counter[op])
            f["somma_fin"] = float(operator_fund[op])
            top_feats.append(f)

        top_pr.addFeatures(top_feats)
        top_tbl.updateExtents()

        # ------------------------------------------------------------
        # 7. Salvataggio GPKG
        # ------------------------------------------------------------

        # Se esiste già, lo elimino prima per evitare problemi di update mode
        if os.path.exists(output_gpkg):
            try:
                os.remove(output_gpkg)
            except Exception as e:
                raise QgsProcessingException(f"Impossibile eliminare il GPKG esistente: {output_gpkg} | {e}")

        # 1° scrittura: crea o sovrascrive l'intero file
        opts = QgsVectorFileWriter.SaveVectorOptions()
        opts.driverName = "GPKG"
        opts.layerName = output_layer_name
        opts.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile

        res1 = QgsVectorFileWriter.writeAsVectorFormatV3(
            out_layer,
            output_gpkg,
            context.transformContext(),
            opts
        )

        if res1[0] != QgsVectorFileWriter.NoError:
            raise QgsProcessingException(f"Errore scrittura layer comuni in GPKG: {res1}")

        # 2° scrittura: aggiunge/sovrascrive il layer tabellare nello stesso GPKG
        opts2 = QgsVectorFileWriter.SaveVectorOptions()
        opts2.driverName = "GPKG"
        opts2.layerName = output_top_name
        opts2.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer

        res2 = QgsVectorFileWriter.writeAsVectorFormatV3(
            top_tbl,
            output_gpkg,
            context.transformContext(),
            opts2
        )

        if res2[0] != QgsVectorFileWriter.NoError:
            raise QgsProcessingException(f"Errore scrittura tabella top in GPKG: {res2}")
             
      
        # ------------------------------------------------------------
        # 8. Ricarica layer
        # ------------------------------------------------------------

        final_uri = f"{output_gpkg}|layername={output_layer_name}"
        top_uri = f"{output_gpkg}|layername={output_top_name}"

        final_loaded = QgsVectorLayer(final_uri, output_layer_name, "ogr")
        top_loaded = QgsVectorLayer(top_uri, output_top_name, "ogr")

        if not final_loaded.isValid():
            raise QgsProcessingException("Layer finale GPKG non valido.")
        if not top_loaded.isValid():
            raise QgsProcessingException("Tabella top GPKG non valida.")

        QgsProject.instance().addMapLayer(final_loaded)
        QgsProject.instance().addMapLayer(top_loaded)

        # ------------------------------------------------------------
        # 9. Tematizzazione
        # ------------------------------------------------------------

        cats = []

        for i, op in enumerate(top_ops):
            sym = QgsSymbol.defaultSymbol(final_loaded.geometryType())
            sym.setColor(self.distinct_color(i))
            sym.setOpacity(0.85)
            cats.append(QgsRendererCategory(op, sym, op))

        sym_altro = QgsSymbol.defaultSymbol(final_loaded.geometryType())
        sym_altro.setColor(QColor("#bdbdbd"))
        sym_altro.setOpacity(0.75)
        cats.append(QgsRendererCategory("ALTRO", sym_altro, "ALTRO"))

        sym_none = QgsSymbol.defaultSymbol(final_loaded.geometryType())
        sym_none.setColor(QColor(255, 255, 255, 0))
        sym_none.setOpacity(0.0)
        cats.append(QgsRendererCategory("NESSUN_MATCH", sym_none, "NESSUN_MATCH"))

        renderer = QgsCategorizedSymbolRenderer("top20_agg", cats)
        final_loaded.setRenderer(renderer)
        final_loaded.triggerRepaint()

        # ------------------------------------------------------------
        # 10. Report
        # ------------------------------------------------------------

        feedback.pushInfo("=== REPORT ===")
        feedback.pushInfo(f"Comuni con CUP: {len(comune_to_cup)}")
        feedback.pushInfo(f"CUP con CIG: {len(cup_to_cig)}")
        feedback.pushInfo(f"CIG con aggiudicatario: {len(agg_by_cig)}")

        for k in ["OK", "MULTI", "NO_CUP", "NO_CIG", "NO_AGGIUD"]:
            feedback.pushInfo(f"{k}: {status_counts.get(k, 0)}")

        feedback.pushInfo(f"=== TOP {top_n} ===")
        for i, op in enumerate(top_ops, start=1):
            feedback.pushInfo(
                f"{i:02d}. {op} | comuni={operator_counter[op]} | somma_fin={round(operator_fund[op], 2)}"
            )

        return {
            self.PARAM_OUT_GPKG: output_gpkg
        }