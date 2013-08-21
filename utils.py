"""NDAR utilitites"""

import math
import MySQLdb

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

# eof
