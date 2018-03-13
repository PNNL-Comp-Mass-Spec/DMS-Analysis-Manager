echo Note that MSGF can read .mzXML files but it cannot read .mzML files

echo Required columns in the input file are the following
echo #SpectrumFile	Title	Scan#	Annotation	Charge	Protein_First	Result_ID

echo Additional columns are allowed; they will be included in the result file but are otherwise ignored
echo This is useful for tracking additional information about each PSM

C:\Program Files\Java\jre8\bin\java.exe -Xmx4000M -cp C:\DMS_Programs\MSGFDB\MSGFDB.jar ui.MSGF -i C:\DMS_WorkDir\Examine_xt_MSGF_input_1.txt -d C:\DMS_WorkDir -o C:\DMS_WorkDir\HCC-38_DDDT_4xdil_20uL_3hr_3_08Jan16_Pippin_15-08-53_xt_MSGF_1.txt -m 0 -e 1 -fixMod 0 -x 0 -p 1
