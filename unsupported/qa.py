#!/usr/bin/python

import sys
import os
import subprocess
import nipype.pipeline.engine as np_pe
import nipype.interfaces.fsl as np_fsl

class StructuralQAWorkflow(np_pe.Workflow):

    def __init__(self, *args, **kwargs):

        in_file = kwargs['in_file']
        del kwargs['in_file']
        np_pe.Workflow.__init__(self, *args, **kwargs)

        reorient = np_pe.Node(interface=np_fsl.Reorient2Std(), name='reorient')
        reorient.inputs.in_file = in_file
        bet = np_pe.Node(interface=np_fsl.BET(), name='bet')
        bet.inputs.mask = True
        bet.inputs.surfaces = True
        self.connect(reorient, 'out_file', bet, 'in_file')

        fast = np_pe.Node(interface=np_fsl.FAST(), name='fast')
        fast.inputs.img_type = 1
        self.connect(bet, 'out_file', fast, 'in_files')

        generate_external = np_pe.Node(interface=np_fsl.ImageMaths(), 
                                       name='generate_external')
        generate_external.inputs.op_string = '-sub 1 -mul -1'
        self.connect(bet, 'outskin_mask_file', generate_external, 'in_file')

        generate_csf = np_pe.Node(interface=np_fsl.ImageMaths(), name='generate_csf')
        generate_csf.inputs.op_string = '-thr 1 -uthr 1 -bin'
        self.connect(fast, 'tissue_class_map', generate_csf, 'in_file')

        generate_gm = np_pe.Node(interface=np_fsl.ImageMaths(), name='generate_gm')
        generate_gm.inputs.op_string = '-thr 2 -uthr 2 -bin'
        self.connect(fast, 'tissue_class_map', generate_gm, 'in_file')

        generate_wm = np_pe.Node(interface=np_fsl.ImageMaths(), name='generate_wm')
        generate_wm.inputs.op_string = '-thr 3 -uthr 3 -bin'
        self.connect(fast, 'tissue_class_map', generate_wm, 'in_file')

        external_stats = np_pe.Node(interface=np_fsl.ImageStats(), 
                                    name='external_stats')
        external_stats.inputs.op_string = '-k %s -R -r -m -s -v'
        self.connect(reorient, 'out_file', external_stats, 'in_file')
        self.connect(generate_external, 'out_file', external_stats, 'mask_file')

        brain_stats = np_pe.Node(interface=np_fsl.ImageStats(), name='brain_stats')
        brain_stats.inputs.op_string = '-k %s -R -r -m -s -v'
        self.connect(reorient, 'out_file', brain_stats, 'in_file')
        self.connect(bet, 'mask_file', brain_stats, 'mask_file')

        csf_stats = np_pe.Node(interface=np_fsl.ImageStats(), name='csf_stats')
        csf_stats.inputs.op_string = '-k %s -R -r -m -s -v'
        self.connect(reorient, 'out_file', csf_stats, 'in_file')
        self.connect(generate_csf, 'out_file', csf_stats, 'mask_file')

        gm_stats = np_pe.Node(interface=np_fsl.ImageStats(), name='gm_stats')
        gm_stats.inputs.op_string = '-k %s -R -r -m -s -v'
        self.connect(reorient, 'out_file', gm_stats, 'in_file')
        self.connect(generate_gm, 'out_file', gm_stats, 'mask_file')

        wm_stats = np_pe.Node(interface=np_fsl.ImageStats(), name='wm_stats')
        wm_stats.inputs.op_string = '-k %s -R -r -m -s -v'
        self.connect(reorient, 'out_file', wm_stats, 'in_file')
        self.connect(generate_wm, 'out_file', wm_stats, 'mask_file')

        return

    def run(self):
        return np_pe.Workflow.run(self)

def run_time_series_qa(xcede_file, output_dir):
    args = ['fmriqa_generate.pl', '--verbose', xcede_file, output_dir]
    print args
    subprocess.check_call(args)
    return

def run_diffusion_qa(nrrd_file, base_dir):
    cwd = os.getcwd()
    os.chdir(base_dir)
    try:
        args = ['DTIPrep', '-w', nrrd_file, '-p', 'default', '-d', '-c']
        subprocess.check_call(args)
    finally:
        os.chdir(cwd)
    return

# eof
