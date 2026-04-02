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
import unicodedata
from collections import defaultdict, Counter


class AnncsuAggiudicatariAlgorithmV22(QgsProcessingAlgorithm):
    PARAM_COMUNI = "COMUNI"
    PARAM_URL_CAND = "URL_CAND"
    PARAM_CUP = "CUP"
    PARAM_AGG = "AGG"
    PARAM_TOPN = "TOPN"
    PARAM_OUT_GPKG = "OUT_GPKG"
    PARAM_OUT_LAYER_NAME = "OUT_LAYER_NAME"
    PARAM_OUT_DETAIL_NAME = "OUT_DETAIL_NAME"
    PARAM_OUT_TOP_NAME = "OUT_TOP_NAME"

    def tr(self, string):
        return string

    def createInstance(self):
        return AnncsuAggiudicatariAlgorithmV22()

    def name(self):
        return "anncsu_aggiudicatari_processing_v22"

    def displayName(self):
        return "ANNCSU aggiudicatari comuni v2.2"

    def group(self):
        return "Custom scripts"

    def groupId(self):
        return "customscripts"

    def shortHelpString(self):
        return (
            "Incrocia candidature ANNCSU, CUP, CIG e aggiudicatari ANAC. "
            "Gestisce più CIG per CUP, crea layer comuni aggregato, "
            "layer dettaglio CIG e tabella top operatori. "
            "Usa join ISTAT e fallback su nome comune normalizzato."
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
                "Nome layer comuni",
                defaultValue="comuni_anncsu_aggiudicatari"
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PARAM_OUT_DETAIL_NAME,
                "Nome layer dettaglio CIG",
                defaultValue="comuni_anncsu_cig_dettaglio"
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

    def normalize_name(self, s):
        s = self.safe_str(s).lower().strip()

        s = s.replace("’", "'").replace("`", "'").replace("´", "'")

        s = "".join(
            ch for ch in unicodedata.normalize("NFD", s)
            if unicodedata.category(ch) != "Mn"
        )

        cleaned = []
        for ch in s:
            if ch.isalnum() or ch.isspace():
                cleaned.append(ch)
            elif ch == "'":
                continue
            else:
                cleaned.append(" ")

        s = "".join(cleaned)

        if s.startswith("comune di "):
            s = s[len("comune di "):]

        s = " ".join(s.split())
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

    def get_common_name_field(self, layer):
        for cand in ["COMUNE", "comune", "DENOMINAZIONE", "denominazione", "NOME", "nome"]:
            idx = layer.fields().indexOf(cand)
            if idx != -1:
                return cand
        raise QgsProcessingException(
            "Nel layer comuni non trovo il campo nome comune (es. COMUNE)."
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
        output_detail_name = self.parameterAsString(parameters, self.PARAM_OUT_DETAIL_NAME, context)
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

        feedback.pushInfo("=== Avvio elaborazione v2.2 multi-CIG + join nome comune ===")
        feedback.pushInfo(f"Cartella di lavoro: {work_dir}")

        # ------------------------------------------------------------
        # 1. Candidature
        # ------------------------------------------------------------

        cand_local = os.path.join(work_dir, "candidature_finanziate_131.csv")
        self.download_to_file(url_cand, cand_local, feedback)

        cand_reader = self.csv_reader_from_any(cand_local, work_dir, feedback)
        cand_fields = cand_reader.fieldnames or []

        cand_codice_comune = self.detect_field(cand_fields, ["cod_comune"], "candidature")
        cand_comune = self.detect_field(cand_fields, ["comune"], "candidature")
        cand_avviso = self.detect_field(cand_fields, ["avviso"], "candidature")
        cand_cup = self.detect_field(cand_fields, ["codice_cup", "cup"], "candidature")
        cand_importo = self.detect_field(cand_fields, ["importo_finanziamento"], "candidature")

        comune_to_cup = {}
        comune_to_importo = defaultdict(float)

        comune_name_to_cup = {}
        comune_name_to_importo = defaultdict(float)

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
            comune_nome = self.normalize_name(row.get(cand_comune))
            cup = self.norm_text(row.get(cand_cup))
            importo = self.parse_number(row.get(cand_importo))

            if not cup:
                continue

            if codice:
                if codice not in comune_to_cup:
                    comune_to_cup[codice] = cup
                comune_to_importo[codice] += importo

            if comune_nome:
                if comune_nome not in comune_name_to_cup:
                    comune_name_to_cup[comune_nome] = cup
                comune_name_to_importo[comune_nome] += importo

        feedback.pushInfo(f"Candidature lette: {rows_total}")
        feedback.pushInfo(f"Candidature ANNCSU: {rows_anncsu}")
        feedback.pushInfo(f"Comuni ANNCSU con CUP via ISTAT: {len(comune_to_cup)}")
        feedback.pushInfo(f"Comuni ANNCSU con CUP via nome: {len(comune_name_to_cup)}")

        # ------------------------------------------------------------
        # 2. CUP -> CIG (multi)
        # ------------------------------------------------------------

        cup_reader = self.csv_reader_from_any(cup_csv_o_zip, work_dir, feedback, preferred_member_contains="cup")
        cup_fields = cup_reader.fieldnames or []

        cup_field = self.detect_field(cup_fields, ["cup", "codice_cup"], "cup")
        cig_field_in_cup = self.detect_field(cup_fields, ["cig", "codice_cig"], "cup")

        wanted_cups = set(comune_to_cup.values()) | set(comune_name_to_cup.values())
        cup_to_cigs = defaultdict(list)

        feedback.pushInfo("Indicizzazione CUP -> CIG (multi)...")

        for row in cup_reader:
            cup = self.norm_text(row.get(cup_field))
            if cup not in wanted_cups:
                continue

            cig = self.norm_text(row.get(cig_field_in_cup))
            if cig and cig not in cup_to_cigs[cup]:
                cup_to_cigs[cup].append(cig)

        cups_with_cig = sum(1 for c in wanted_cups if len(cup_to_cigs.get(c, [])) > 0)
        feedback.pushInfo(f"CUP richiesti: {len(wanted_cups)}")
        feedback.pushInfo(f"CUP con almeno un CIG: {cups_with_cig}")

        # ------------------------------------------------------------
        # 3. Aggiudicatari ANAC
        # ------------------------------------------------------------

        agg_reader = self.csv_reader_from_any(aggiudicatari_csv, work_dir, feedback, preferred_member_contains="aggiudicatari")
        agg_fields = agg_reader.fieldnames or []

        agg_cig = self.detect_field(agg_fields, ["cig", "codice_cig"], "aggiudicatari")
        agg_den = self.detect_field(agg_fields, ["denominazione", "ragione_sociale", "aggiudicatario"], "aggiudicatari")
        agg_ruolo = self.choose_first_existing(agg_fields, ["ruolo"])

        wanted_cigs = set()
        for cigs in cup_to_cigs.values():
            for cig in cigs:
                wanted_cigs.add(cig)

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
        feedback.pushInfo(f"CIG con aggiudicatario ANAC: {len(agg_by_cig)}")

        # ------------------------------------------------------------
        # 4. Layer output - comuni
        # ------------------------------------------------------------

        pro_com_t = self.get_common_code_field(comuni)
        comune_nome_field = self.get_common_name_field(comuni)

        comuni_fields = self.clone_fields(comuni)
        for extra in [
            ("istat_match", QMetaType.Type.QString, 20, 0),
            ("cup_match", QMetaType.Type.QString, 64, 0),
            ("cig_match", QMetaType.Type.QString, 1000, 0),
            ("match_status", QMetaType.Type.QString, 32, 0),
            ("fonte_match", QMetaType.Type.QString, 64, 0),
            ("fin_comune", QMetaType.Type.Double, 20, 2),
            ("top20_agg", QMetaType.Type.QString, 254, 0),
            ("num_cig", QMetaType.Type.Int, 10, 0),
            ("num_match_cig", QMetaType.Type.Int, 10, 0),
            ("operatori_all", QMetaType.Type.QString, 1000, 0),
            ("join_tipo", QMetaType.Type.QString, 20, 0),
            ("comune_norm", QMetaType.Type.QString, 254, 0),
        ]:
            self.add_field_if_absent(comuni_fields, extra[0], extra[1], extra[2], extra[3])

        for f in agg_fields:
            self.add_field_if_absent(comuni_fields, f"agg_{f}", QMetaType.Type.QString, 254, 0)

        mem_uri = f"Polygon?crs={comuni.crs().authid()}"
        out_layer = QgsVectorLayer(mem_uri, output_layer_name, "memory")
        out_pr = out_layer.dataProvider()
        out_pr.addAttributes(comuni_fields)
        out_layer.updateFields()

        # ------------------------------------------------------------
        # 5. Layer output - dettaglio CIG
        # ------------------------------------------------------------

        detail_fields = self.clone_fields(comuni)
        for extra in [
            ("istat_match", QMetaType.Type.QString, 20, 0),
            ("cup_match", QMetaType.Type.QString, 64, 0),
            ("cig_match", QMetaType.Type.QString, 64, 0),
            ("fonte_match", QMetaType.Type.QString, 64, 0),
            ("fin_comune", QMetaType.Type.Double, 20, 2),
            ("operatore", QMetaType.Type.QString, 254, 0),
            ("num_operatori_cig", QMetaType.Type.Int, 10, 0),
            ("join_tipo", QMetaType.Type.QString, 20, 0),
            ("comune_norm", QMetaType.Type.QString, 254, 0),
        ]:
            self.add_field_if_absent(detail_fields, extra[0], extra[1], extra[2], extra[3])

        for f in agg_fields:
            self.add_field_if_absent(detail_fields, f"agg_{f}", QMetaType.Type.QString, 254, 0)

        detail_layer = QgsVectorLayer(mem_uri, output_detail_name, "memory")
        detail_pr = detail_layer.dataProvider()
        detail_pr.addAttributes(detail_fields)
        detail_layer.updateFields()

        status_counts = Counter()
        operator_counter = Counter()
        operator_fund = defaultdict(float)

        feedback.pushInfo("Costruzione layer finale comuni e layer dettaglio CIG...")

        new_feats = []
        detail_feats = []

        for ft in comuni.getFeatures():
            codice = self.normalize_code(ft[pro_com_t])
            comune_nome_norm = self.normalize_name(ft[comune_nome_field])

            nf = QgsFeature(out_layer.fields())
            nf.setGeometry(ft.geometry())

            for f in comuni.fields():
                nf[f.name()] = ft[f.name()]

            nf["istat_match"] = codice
            nf["cup_match"] = None
            nf["cig_match"] = None
            nf["match_status"] = "NESSUN_MATCH"
            nf["fonte_match"] = "NO_MATCH"
            nf["top20_agg"] = None
            nf["num_cig"] = 0
            nf["num_match_cig"] = 0
            nf["operatori_all"] = None
            nf["join_tipo"] = None
            nf["comune_norm"] = comune_nome_norm

            cup = None
            fin_comune_val = 0.0
            join_tipo = None

            if codice and codice in comune_to_cup:
                cup = comune_to_cup.get(codice)
                fin_comune_val = comune_to_importo.get(codice, 0.0)
                join_tipo = "ISTAT"
            elif comune_nome_norm and comune_nome_norm in comune_name_to_cup:
                cup = comune_name_to_cup.get(comune_nome_norm)
                fin_comune_val = comune_name_to_importo.get(comune_nome_norm, 0.0)
                join_tipo = "NOME"

            nf["fin_comune"] = float(fin_comune_val)
            nf["join_tipo"] = join_tipo

            if not cup:
                nf["match_status"] = "NO_CUP"
                status_counts["NO_CUP"] += 1
                new_feats.append(nf)
                continue

            nf["cup_match"] = cup

            cigs = cup_to_cigs.get(cup, [])
            nf["num_cig"] = len(cigs)

            if not cigs:
                nf["match_status"] = "NO_CIG"
                status_counts["NO_CIG"] += 1
                new_feats.append(nf)
                continue

            matched_cigs = []
            operator_counter_local = Counter()
            operator_best_row = {}

            for cig in cigs:
                agg_row = agg_by_cig.get(cig)
                if not agg_row:
                    continue

                matched_cigs.append(cig)

                operatore = self.norm_text(agg_row.get(agg_den))
                if operatore:
                    operator_counter_local[operatore] += 1
                    if operatore not in operator_best_row:
                        operator_best_row[operatore] = agg_row

                df = QgsFeature(detail_layer.fields())
                df.setGeometry(ft.geometry())

                for f in comuni.fields():
                    df[f.name()] = ft[f.name()]

                df["istat_match"] = codice
                df["cup_match"] = cup
                df["cig_match"] = cig
                df["fonte_match"] = "ANAC_AGGIUDICATARI"
                df["fin_comune"] = float(fin_comune_val)
                df["operatore"] = operatore
                df["num_operatori_cig"] = 1 + int(multi_count.get(cig, 0))
                df["join_tipo"] = join_tipo
                df["comune_norm"] = comune_nome_norm

                for f in agg_fields:
                    df[f"agg_{f}"] = self.safe_str(agg_row.get(f))

                detail_feats.append(df)

            nf["num_match_cig"] = len(matched_cigs)
            nf["cig_match"] = "|".join(matched_cigs[:50]) if matched_cigs else None

            if not matched_cigs:
                nf["match_status"] = "NO_AGGIUD"
                nf["fonte_match"] = "NO_MATCH"
                status_counts["NO_AGGIUD"] += 1
                new_feats.append(nf)
                continue

            dominant_operator = sorted(
                operator_counter_local.keys(),
                key=lambda op: (-operator_counter_local[op], op)
            )[0] if operator_counter_local else None

            nf["operatori_all"] = " | ".join(
                [f"{op} ({cnt})" for op, cnt in operator_counter_local.most_common(10)]
            ) if operator_counter_local else None

            nf["fonte_match"] = "ANAC_AGGIUDICATARI"

            if dominant_operator:
                best_row = operator_best_row[dominant_operator]
                for f in agg_fields:
                    nf[f"agg_{f}"] = self.safe_str(best_row.get(f))

                idx_agg_den = out_layer.fields().indexOf("agg_denominazione")
                idx_agg_rs = out_layer.fields().indexOf("agg_ragione_sociale")

                if idx_agg_den != -1:
                    nf["agg_denominazione"] = dominant_operator
                elif idx_agg_rs != -1:
                    nf["agg_ragione_sociale"] = dominant_operator

                nf["match_status"] = "OK"
                status_counts["OK"] += 1

                operator_counter[dominant_operator] += 1
                operator_fund[dominant_operator] += fin_comune_val
            else:
                nf["match_status"] = "NO_AGGIUD"
                status_counts["NO_AGGIUD"] += 1

            new_feats.append(nf)

        out_pr.addFeatures(new_feats)
        out_layer.updateExtents()

        detail_pr.addFeatures(detail_feats)
        detail_layer.updateExtents()

        # ------------------------------------------------------------
        # 6. Top N
        # ------------------------------------------------------------

        ranking = sorted(
            operator_counter.keys(),
            key=lambda op: (-operator_counter[op], -operator_fund[op], op)
        )
        top_ops = ranking[:top_n]
        top_set = set(top_ops)

        out_layer.startEditing()
        idx_top = out_layer.fields().indexOf("top20_agg")

        idx_den = out_layer.fields().indexOf("agg_denominazione")
        idx_alt = out_layer.fields().indexOf("agg_ragione_sociale")

        for ft in out_layer.getFeatures():
            den = ""
            if idx_den != -1:
                den = self.norm_text(ft[idx_den])
            elif idx_alt != -1:
                den = self.norm_text(ft[idx_alt])

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
        # 7. Tabella top
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
        # 8. Salvataggio GPKG
        # ------------------------------------------------------------

        if os.path.exists(output_gpkg):
            try:
                os.remove(output_gpkg)
            except Exception as e:
                raise QgsProcessingException(f"Impossibile eliminare il GPKG esistente: {output_gpkg} | {e}")

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

        opts_detail = QgsVectorFileWriter.SaveVectorOptions()
        opts_detail.driverName = "GPKG"
        opts_detail.layerName = output_detail_name
        opts_detail.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer

        res_detail = QgsVectorFileWriter.writeAsVectorFormatV3(
            detail_layer,
            output_gpkg,
            context.transformContext(),
            opts_detail
        )

        if res_detail[0] != QgsVectorFileWriter.NoError:
            raise QgsProcessingException(f"Errore scrittura layer dettaglio in GPKG: {res_detail}")

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
        # 9. Ricarica layer
        # ------------------------------------------------------------

        final_uri = f"{output_gpkg}|layername={output_layer_name}"
        detail_uri = f"{output_gpkg}|layername={output_detail_name}"
        top_uri = f"{output_gpkg}|layername={output_top_name}"

        final_loaded = QgsVectorLayer(final_uri, output_layer_name, "ogr")
        detail_loaded = QgsVectorLayer(detail_uri, output_detail_name, "ogr")
        top_loaded = QgsVectorLayer(top_uri, output_top_name, "ogr")

        if not final_loaded.isValid():
            raise QgsProcessingException("Layer finale GPKG non valido.")
        if not detail_loaded.isValid():
            raise QgsProcessingException("Layer dettaglio GPKG non valido.")
        if not top_loaded.isValid():
            raise QgsProcessingException("Tabella top GPKG non valida.")

        QgsProject.instance().addMapLayer(final_loaded)
        QgsProject.instance().addMapLayer(detail_loaded)
        QgsProject.instance().addMapLayer(top_loaded)

        # ------------------------------------------------------------
        # 10. Tematizzazione layer comuni
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
        # 11. Report
        # ------------------------------------------------------------

        fonte_counter = Counter()
        multi_cig_comuni = 0
        multi_match_cig_comuni = 0
        join_nome_count = 0

        for ft in out_layer.getFeatures():
            fonte_counter[self.safe_str(ft["fonte_match"])] += 1
            if self.safe_str(ft["join_tipo"]) == "NOME":
                join_nome_count += 1
            try:
                if int(ft["num_cig"]) > 1:
                    multi_cig_comuni += 1
            except:
                pass
            try:
                if int(ft["num_match_cig"]) > 1:
                    multi_match_cig_comuni += 1
            except:
                pass

        feedback.pushInfo("=== REPORT ===")
        feedback.pushInfo(f"Comuni con CUP via ISTAT: {len(comune_to_cup)}")
        feedback.pushInfo(f"Comuni con CUP via nome: {len(comune_name_to_cup)}")
        feedback.pushInfo(f"CUP con almeno un CIG: {cups_with_cig}")
        feedback.pushInfo(f"CIG richiesti complessivi: {len(wanted_cigs)}")
        feedback.pushInfo(f"CIG con aggiudicatario ANAC: {len(agg_by_cig)}")
        feedback.pushInfo(f"Comuni con più di un CIG: {multi_cig_comuni}")
        feedback.pushInfo(f"Comuni con più di un CIG matchato: {multi_match_cig_comuni}")
        feedback.pushInfo(f"Feature layer dettaglio CIG: {len(detail_feats)}")
        feedback.pushInfo(f"Comuni recuperati con join su nome: {join_nome_count}")

        for k in ["OK", "NO_CUP", "NO_CIG", "NO_AGGIUD"]:
            feedback.pushInfo(f"{k}: {status_counts.get(k, 0)}")

        feedback.pushInfo("=== FONTI MATCH ===")
        for k, v in fonte_counter.items():
            feedback.pushInfo(f"{k}: {v}")

        feedback.pushInfo(f"=== TOP {top_n} ===")
        for i, op in enumerate(top_ops, start=1):
            feedback.pushInfo(
                f"{i:02d}. {op} | comuni={operator_counter[op]} | somma_fin={round(operator_fund[op], 2)}"
            )

        return {
            self.PARAM_OUT_GPKG: output_gpkg
        }