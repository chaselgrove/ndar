"""NDAR utilitites

attr_value, HTMLTable, and BIRNParser are taken from the 
INCF one-click data sharing project: https://github.com/incf/one_click
"""

import math
import xml.dom.minidom
import HTMLParser
import MySQLdb

def attr_value(attrs, name, default=None):
    for (n, v) in attrs:
        if n == name:
            return v
    return default

class HTMLTable:

    def __init__(self, id):
        self.id = id
        # self.cells[row][col] = data
        self.cells = {}
        self.row = None
        self.col = None
        self.rowspan = None
        self.colspan = None
        self.cell = None
        return

    def __iter__(self):
        max_row = max(self.cells)
        max_col = 0
        for r in xrange(max_row+1):
            mc = max(self.cells[r])
            if max_col < mc:
                max_col = mc
        for row in range(max_row+1):
            cols = []
            for col in range(max_col+1):
                try:
                    cols.append(self.cells[row][col])
                except KeyError:
                    cols.append(None)
            yield cols
        return

    def tr(self):
        if self.row is None:
            self.row = 0
        else:
            self.row += 1
        return

    def td(self, attrs):
        self.rowspan = int(attr_value(attrs, 'rowspan', 1))
        self.colspan = int(attr_value(attrs, 'colspan', 1))
        self.cell = ''
        if self.row not in self.cells:
            self.col = 0
        else:
            if self.col is None:
                self.col = 0
            while True:
                if self.col not in self.cells[self.row]:
                    break
                self.col += 1
        return

    def data(self, data):
        self.cell += data
        return

    def end_td(self):
        for r in range(self.row, self.row+self.rowspan):
            for c in range(self.col, self.col+self.colspan):
                self.cells.setdefault(r, {})
                self.cells[r][c] = self.cell
        self.rowspan = None
        self.colspan = None
        self.cell = None
        return

    def end_tr(self):
        self.col = None
        return

class BIRNParser(HTMLParser.HTMLParser):

    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.state = None
        self.table = None
        self.data = {}
        return

    def handle_starttag(self, tag, attrs):
        # occasionally a </td> is missing, so we get <td>data<td>more data; 
        # handle that case here (simulate the missing </td>)
        if self.state == 'td' and tag == 'td':
            self.handle_endtag('td')
        if not self.state:
            if tag == 'table':
                self.table = HTMLTable(attr_value(attrs, 'id'))
                self.state = 'table'
        elif self.state == 'table':
            if tag == 'tr':
                self.table.tr()
                self.state = 'tr'
        elif self.state == 'tr':
            if tag == 'td':
                self.table.td(attrs)
                self.state = 'td'
        return

    def handle_data(self, data):
        if self.state == 'td':
            self.table.data(data)
        return

    def handle_endtag(self, tag):
        if self.state == 'table':
            if tag == 'table':
                self.read_table()
                self.table = None
                self.state = None
        elif self.state == 'tr':
            if tag == 'tr':
                self.table.end_tr()
                self.state = 'table'
        elif self.state == 'td':
            if tag == 'td':
                self.table.end_td()
                self.state = 'tr'
        return

    def read_table(self):
        if self.table.id == 'table_top_summary':
            for row in self.table:
                if row[0] == 'input':
                    self.take_summary_input_value(row)
                elif row[0] == 'masked':
                    self.take_summary_masked_value(row)
                elif row[0] == 'masked, detrended':
                    self.take_summary_md_value(row)
        elif self.table.id is None:
            pass
        elif self.table.id.startswith('table_') and \
             self.table.id.endswith('_summary'):
            self.read_summary_table(self.table.id[6:-8])
        elif self.table.id.startswith('qa_data_'):
            self.read_data_table(self.table.id[8:])
        return

    def read_summary_table(self, name):
        self.data[name] = {}
        for row in self.table:
            if row[0] == 'Mean:':
                if row[1] == '(absolute)\n':
                    self.data[name]['abs_mean'] = row[2]
                elif row[1] == '(relative)':
                    self.data[name]['rel_mean'] = row[2]
        return

    def read_data_table(self, name):
        data = []
        for row in self.table:
            if row[0] == 'VOLNUM':
                continue
            data.append(row)
        self.data[name]['data'] = data
        return

    def take_summary_input_value(self, row):
        if row[1] == '# potentially-clipped voxels':
            self.input_pcv = row[3]
        elif row[1] == '# vols. with mean intensity abs. z-score > 3':
            if row[2] == 'individual':
                self.input_nvmiaz3_ind = row[3]
            elif row[2] == 'rel. to grand mean':
                self.input_nvmiaz3_rgm = row[3]
        elif row[1] == '# vols. with mean intensity abs. z-score > 4':
            if row[2] == 'individual':
                self.input_nvmiaz4_ind = row[3]
            elif row[2] == 'rel. to grand mean':
                self.input_nvmiaz4_rgm = row[3]
        elif row[1] == '# vols. with mean volume difference > 1%':
            self.input_nvmvd1 = row[3]
        elif row[1] == '# vols. with mean volume difference > 2%':
            self.input_nvmvd2 = row[3]
        return

    def take_summary_masked_value(self, row):
        if row[1] == 'mean FWHM':
            if row[2] == 'X':
                self.masked_fwhm_x = row[3]
            elif row[2] == 'Y':
                self.masked_fwhm_y = row[3]
            elif row[2] == 'Z':
                self.masked_fwhm_z = row[3]
        return

    def take_summary_md_value(self, row):
        if row[1] == '# vols. with mean intensity abs. z-score > 3':
            if row[2] == 'individual':
                self.md_nvmiaz3_ind = row[3]
            elif row[2] == 'rel. to grand mean':
                self.md_nvmiaz3_rgm = row[3]
        elif row[1] == '# vols. with mean intensity abs. z-score > 4':
            if row[2] == 'individual':
                self.md_nvmiaz4_ind = row[3]
            elif row[2] == 'rel. to grand mean':
                self.md_nvmiaz4_rgm = row[3]
        elif row[1] == '# vols. with running difference > 1%':
            self.md_nvrd1 = row[3]
        elif row[1] == '# vols. with running difference > 2%':
            self.md_nvrd2 = row[3]
        elif row[1] == '# vols. with > 1% outlier voxels':
            self.md_nv1ov = row[3]
        elif row[1] == '# vols. with > 2% outlier voxels':
            self.md_nv2ov = row[3]
        elif row[1] == 'mean (ROI in middle slice)':
            self.md_mroims = row[3]
        elif row[1] == 'mean SNR (ROI in middle slice)':
            self.md_msnroims = row[3]
        elif row[1] == 'mean SFNR (ROI in middle slice)':
            self.md_msfnrroims = row[3]
        return

def store_structural_qa(image, graph, 
                        db_host, db_user, db_password, database):

    db = MySQLdb.connect(db_host, db_user, db_password, database)
    c = db.cursor()

    vals = {}
    val_types = ('min', 'max', 'robust_min', 'robust_max', 
                 'mean', 'std', 'voxels', 'volume')

    for tissue in ('brain', 'csf', 'gm', 'wm', 'external'):
        for node in graph.nodes():
            if node.name == '%s_stats' % tissue:
                val_list = []
                for v in node.result.outputs.out_stat:
                    if math.isnan(v):
                        val_list.append(None)
                    else:
                        val_list.append(v)
                break
        vals[tissue] = dict(zip(val_types, val_list))

    try:
        snr = vals['brain']['mean'] / vals['external']['std']
    except:
        # error could be NaNs or divide by zero
        snr = None

    query = """INSERT INTO imaging_qa01 (subjectkey, 
                                         src_subject_id, 
                                         interview_date, 
                                         interview_age, 
                                         gender, 
                                         file_source, 
                                         external_min, 
                                         external_max, 
                                         external_robust_min, 
                                         external_robust_max, 
                                         external_mean, 
                                         external_std, 
                                         external_voxels, 
                                         external_volume, 
                                         brain_min, 
                                         brain_max, 
                                         brain_robust_min, 
                                         brain_robust_max, 
                                         brain_mean, 
                                         brain_std, 
                                         brain_voxels, 
                                         brain_volume, 
                                         csf_min, 
                                         csf_max, 
                                         csf_robust_min, 
                                         csf_robust_max, 
                                         csf_mean, 
                                         csf_std, 
                                         csf_voxels, 
                                         csf_volume, 
                                         gm_min, 
                                         gm_max, 
                                         gm_robust_min, 
                                         gm_robust_max, 
                                         gm_mean, 
                                         gm_std, 
                                         gm_voxels, 
                                         gm_volume, 
                                         wm_min, 
                                         wm_max, 
                                         wm_robust_min, 
                                         wm_robust_max, 
                                         wm_mean, 
                                         wm_std, 
                                         wm_voxels, 
                                         wm_volume, 
                                         snr) 
               VALUES (%s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s)"""
    query_params = (image.subjectkey, 
                    image.src_subject_id, 
                    image.interview_date, 
                    image.interview_age, 
                    image.gender, 
                    image.image_file, 
                    vals['external']['min'], 
                    vals['external']['max'], 
                    vals['external']['robust_min'], 
                    vals['external']['robust_max'], 
                    vals['external']['mean'], 
                    vals['external']['std'], 
                    vals['external']['voxels'], 
                    vals['external']['volume'], 
                    vals['brain']['min'], 
                    vals['brain']['max'], 
                    vals['brain']['robust_min'], 
                    vals['brain']['robust_max'], 
                    vals['brain']['mean'], 
                    vals['brain']['std'], 
                    vals['brain']['voxels'], 
                    vals['brain']['volume'], 
                    vals['csf']['min'], 
                    vals['csf']['max'], 
                    vals['csf']['robust_min'], 
                    vals['csf']['robust_max'], 
                    vals['csf']['mean'], 
                    vals['csf']['std'], 
                    vals['csf']['voxels'], 
                    vals['csf']['volume'], 
                    vals['gm']['min'], 
                    vals['gm']['max'], 
                    vals['gm']['robust_min'], 
                    vals['gm']['robust_max'], 
                    vals['gm']['mean'], 
                    vals['gm']['std'], 
                    vals['gm']['voxels'], 
                    vals['gm']['volume'], 
                    vals['wm']['min'], 
                    vals['wm']['max'], 
                    vals['wm']['robust_min'], 
                    vals['wm']['robust_max'], 
                    vals['wm']['mean'], 
                    vals['wm']['std'], 
                    vals['wm']['voxels'], 
                    vals['wm']['volume'], 
                    snr)

    c.execute(query, query_params)
    db.commit()

    db.close()

    return

def store_time_series_qa(image, index_fname, 
                         db_host, db_user, db_password, database):

    # the second argument is the path to index.html generated 
    # by fmriqa_generate.pl

    birn_parser = BIRNParser()
    birn_parser.feed(open(index_fname).read())

    db = MySQLdb.connect(db_host, db_user, db_password, database)
    c = db.cursor()

    query = """INSERT INTO imaging_qa01 (subjectkey, 
                                         src_subject_id, 
                                         interview_date, 
                                         interview_age, 
                                         gender, 
                                         file_source, 
                                         input_pot_clipped_voxels, 
                                         input_vols_mi_abs_z_3_ind, 
                                         input_vols_mi_abs_z_3_rgm, 
                                         input_vols_mi_abs_z_4_ind, 
                                         input_vols_mi_abs_z_4_rgm, 
                                         input_vols_mvd_1, 
                                         input_vols_mvd_2, 
                                         masked_mean_fwhm_x, 
                                         masked_mean_fwhm_y, 
                                         masked_mean_fwhm_z, 
                                         masked_detr_vols_run_diff_1, 
                                         masked_detr_vols_run_diff_2, 
                                         masked_detr_vols_1_outliers, 
                                         masked_detr_vols_2_outliers, 
                                         masked_detrended_mean, 
                                         masked_detr_mean_snr, 
                                         masked_detr_mean_sfnr) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s)"""

    query_params = (image.subjectkey, 
                    image.src_subject_id, 
                    image.interview_date, 
                    image.interview_age, 
                    image.gender, 
                    image.image_file, 
                    birn_parser.input_pcv, 
                    birn_parser.input_nvmiaz3_ind, 
                    birn_parser.input_nvmiaz3_rgm, 
                    birn_parser.input_nvmiaz4_ind, 
                    birn_parser.input_nvmiaz4_rgm, 
                    birn_parser.input_nvmvd1, 
                    birn_parser.input_nvmvd2, 
                    birn_parser.masked_fwhm_x, 
                    birn_parser.masked_fwhm_y, 
                    birn_parser.masked_fwhm_z, 
                    birn_parser.md_nvrd1, 
                    birn_parser.md_nvrd2, 
                    birn_parser.md_nv1ov, 
                    birn_parser.md_nv2ov, 
                    birn_parser.md_mroims, 
                    birn_parser.md_msnroims, 
                    birn_parser.md_msfnrroims)

    c.execute(query, query_params)
    db.commit()

    db.close()

    return

def find_entry(element, parameter):
    """the DTIPrep QA XML uses <entry> tags and parameter attributes 
    to name the tags

    this function takes an XML element and a parameter name and 
    returns the child "entry" element with the matching parameter

    raises ValueError if a matching element is not found
    """
    for ce in element.childNodes:
        if ce.nodeType != ce.ELEMENT_NODE:
            continue
        if ce.nodeName != 'entry':
            continue
        if ce.getAttribute('parameter') == parameter:
            return ce
    raise ValueError('<element parameter="%s"> not found' % parameter)

def entry_value(el):
    """given a DTIPrep QA entry element, return the text of its child <value>"""
    children = el.getElementsByTagName('value')
    if not children:
        return ''
    s = ''
    for cn in children[0].childNodes:
        if cn.nodeType == cn.TEXT_NODE:
            s += cn.data
    return s

def store_diffusion_qa(image, xml_fname, 
                       db_host, db_user, db_password, database):

    # the second argument is the *_XMLQCResult.xml generated by DTIPrep

    doc = xml.dom.minidom.parse(open(xml_fname))
    root = doc.getElementsByTagName('QCResultSettings')[0]

    ii_element = find_entry(root, 'ImageInformation')

    image_origin_check = entry_value(find_entry(ii_element, 'origin'))
    image_space_check = entry_value(find_entry(ii_element, 'space'))
    image_spaced_direction_check = entry_value(find_entry(ii_element, 
                                                          'spacedirection'))
    image_spacing_check = entry_value(find_entry(ii_element, 'spacing'))
    image_size_check = entry_value(find_entry(ii_element, 'size'))

    di_element = find_entry(root, 'DiffusionInformation')

    image_gradient_check = entry_value(find_entry(di_element, 'gradient'))
    diffusion_meas_frame_check = entry_value(find_entry(di_element, 
                                                        'measurementFrame'))

    dwi_c_element = find_entry(root, 'DWI Check')

    diffusion_slicewise_check = entry_value(find_entry(dwi_c_element, 
                                                       'SliceWiseCheck'))
    dwi_interlacewise_check = entry_value(find_entry(dwi_c_element, 
                                                     'InterlaceWiseCheck'))
    dwi_gradientwise_check = entry_value(find_entry(dwi_c_element, 
                                                    'GradientWiseCheck'))

    db = MySQLdb.connect(db_host, db_user, db_password, database)
    c = db.cursor()

    query = """INSERT INTO imaging_qa01 (subjectkey, 
                                         src_subject_id, 
                                         interview_date, 
                                         interview_age, 
                                         gender, 
                                         file_source, 
                                         image_origin_check, 
                                         image_space_check, 
                                         image_spaced_direction_check, 
                                         image_spacing_check, 
                                         image_size_check, 
                                         image_gradient_check, 
                                         diffusion_meas_frame_check, 
                                         diffusion_slicewise_check, 
                                         dwi_interlacewise_check, 
                                         dwi_gradientwise_check) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, %s, %s, %s)"""

    query_params = (image.subjectkey, 
                    image.src_subject_id, 
                    image.interview_date, 
                    image.interview_age, 
                    image.gender, 
                    image.image_file, 
                    image_origin_check, 
                    image_space_check, 
                    image_spaced_direction_check, 
                    image_spacing_check, 
                    image_size_check, 
                    image_gradient_check, 
                    diffusion_meas_frame_check, 
                    diffusion_slicewise_check, 
                    dwi_interlacewise_check, 
                    dwi_gradientwise_check)

    c.execute(query, query_params)
    db.commit()

    db.close()

    return

# eof
