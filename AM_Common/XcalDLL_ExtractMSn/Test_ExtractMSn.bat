rem LCQ_C3
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\Shed_PMA_Biotin_400o2000_010203.RAW
del *.dta
copy lcq_dta.txt Shed_PMA_Biotin_400o2000_010203_lcq_dta.txt
del lcq_dta.txt

rem LCQ_D2
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_07_02_0pt5_500mM-a_25Mar07_Doc_SCX_06-09-01.raw
del *.dta
copy lcq_dta.txt QC_Shew_07_02_0pt5_500mM-a_25Mar07_Doc_SCX_06-09-01_lcq_dta.txt
del lcq_dta.txt

rem LTQ_ETD_1
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\SysVirol_AI_VN1203_1_12hr_2_12Aug09_Eagle_09-05-24.raw
del *.dta
copy lcq_dta.txt SysVirol_AI_VN1203_1_12hr_2_12Aug09_Eagle_09-05-24_lcq_dta.txt
del lcq_dta.txt

rem LTQ_2
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_09_02-pt5-d_10Aug09_Griffin_09-07-16.raw
del *.dta
copy lcq_dta.txt QC_Shew_09_02-pt5-d_10Aug09_Griffin_09-07-16_lcq_dta.txt
del lcq_dta.txt

rem LTQ_4
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_09_02-pt5_2_17Aug09_Owl_09-05-16.raw
del *.dta
copy lcq_dta.txt QC_Shew_09_02-pt5_2_17Aug09_Owl_09-05-16_lcq_dta.txt
del lcq_dta.txt

rem LTQ_Orb_1
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_09_03-pt5-4_10Aug09_Draco_09-05-04.raw
del *.dta
copy lcq_dta.txt QC_Shew_09_03-pt5-4_10Aug09_Draco_09-05-04_lcq_dta.txt
del lcq_dta.txt

rem LTQ_Orb_2
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_09_01-pt5_a_27Apr09_Falcon_09-01-30.Raw
del *.dta
copy lcq_dta.txt QC_Shew_09_01-pt5_a_27Apr09_Falcon_09-01-30_lcq_dta.txt
del lcq_dta.txt

rem VOrbiETD01
extract_msn.exe  -F500 -L1000 -I35 -G1 C:\XcalDLL\TestData\QC_Shew_09_02_200ng_150m_d_4May09_Hawk_09-01-12.raw
del *.dta
copy lcq_dta.txt QC_Shew_09_02_200ng_150m_d_4May09_Hawk_09-01-12_lcq_dta.txt
del lcq_dta.txt
