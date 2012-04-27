'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a Sequest _syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorSequest
    Inherits clsMSGFInputCreator

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <remarks></remarks>
	Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String)

		MyBase.New(strDatasetName, strWorkDir, PHRPReader.clsPHRPReader.ePeptideHitResultType.Sequest)

	End Sub

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for Sequest synopsis files
		mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserSequest.GetPHRPFirstHitsFileName(mDatasetName))
		mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserSequest.GetPHRPSynopsisFileName(mDatasetName))

    End Sub

   Protected Overrides Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
        Dim dblXCorr As Double
        Dim dblDeltaCN As Double

		Dim intCleavageState As Integer
		Dim intCleavageStateAlt As Short

        Dim blnIsProteinTerminus As Boolean
		Dim blnPassesFilters As Boolean

		' Examine the score values and possibly filter out this line

		' Sequest filter rules are relaxed forms of the MTS Peptide DB Minima (Peptide DB minima 6, filter set 149)
		' All data must have DelCn <= 0.25
		' For Partially or fully tryptic, or protein terminal; 
		'    XCorr >= 1.5 for 1+ or 2+
		'    XCorr >= 2.2 for >=3+
		' For non-tryptic:
		'    XCorr >= 1.5 for 1+
		'    XCorr >= 2.0 for 2+
		'    XCorr >= 2.5 for >=3+

		If objPSM.Peptide.StartsWith("-"c) OrElse objPSM.Peptide.EndsWith("-") Then
			blnIsProteinTerminus = True
		Else
			blnIsProteinTerminus = False
		End If

		dblDeltaCN = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserSequest.DATA_COLUMN_DelCn)
		dblXCorr = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserSequest.DATA_COLUMN_XCorr)

		intCleavageState = PHRPReader.clsPeptideCleavageStateCalculator.CleavageStateToShort(objPSM.CleavageState)
		intCleavageStateAlt = CShort(objPSM.GetScoreInt(PHRPReader.clsPHRPParserSequest.DATA_COLUMN_NumTrypticEnds, 0))

		If intCleavageStateAlt > intCleavageState Then
			intCleavageState = intCleavageStateAlt
		End If

		If dblDeltaCN <= 0.25 Then
			If intCleavageState >= 1 OrElse blnIsProteinTerminus Then
				' Partially or fully tryptic, or protein terminal
				If objPSM.Charge = 1 Or objPSM.Charge = 2 Then
					If dblXCorr >= 1.5 Then blnPassesFilters = True
				Else
					' Charge is 3 or higher (or zero)
					If dblXCorr >= 2.2 Then blnPassesFilters = True
				End If
			Else
				' Non-tryptic
				If objPSM.Charge = 1 Then
					If dblXCorr >= 1.5 Then blnPassesFilters = True
				ElseIf objPSM.Charge = 2 Then
					If dblXCorr >= 2.0 Then blnPassesFilters = True
				Else
					' Charge is 3 or higher (or zero)
					If dblXCorr >= 2.5 Then blnPassesFilters = True
				End If
			End If
		End If


		Return blnPassesFilters


	End Function

End Class
