Public Class clsMSGFPlusPSMs

	Public Structure udtPSMType
		Public Peptide As String
		Public SpecEValue As Double
		Public DataLine As String
	End Structure

	' Keys are the protein and peptide (separated by an undercore)
	' Values are the PSM details, including the original data line from the .TSV file
	Private mPSMs As Dictionary(Of String, udtPSMType)

	' Keys are the peptide sequence, values are the best SpecEValue for that peptide
	Private ReadOnly mPeptides As Dictionary(Of String, Double)
	Private mWorstSpecEValue As Double

	Private ReadOnly mMaximumPSMsToKeep As Integer

	Public ReadOnly Property MaximumPSMsToKeep() As Integer
		Get
			Return mMaximumPSMsToKeep
		End Get
	End Property

	Public ReadOnly Property PSMs() As List(Of udtPSMType)
		Get
			Return mPSMs.Values.ToList()
		End Get
	End Property

	Public Sub New(ByVal maximumPSMsToKeep As Integer)
		mMaximumPSMsToKeep = maximumPSMsToKeep
		If mMaximumPSMsToKeep < 1 Then mMaximumPSMsToKeep = 1

		mPSMs = New Dictionary(Of String, udtPSMType)
		mPeptides = New Dictionary(Of String, Double)

		mWorstSpecEValue = 0
	End Sub

	''' <summary>
	''' Adds the given PSM if the list has fewer than MaximumPSMsToKeep PSMs, or if the specEValue is less than the worst scoring entry in the list
	''' </summary>
	''' <param name="peptide"></param>
	''' <param name="protein"></param>
	''' <param name="specEValue"></param>
	''' <param name="dataLine"></param>
	''' <returns>True if the PSM was stored, otherwise false</returns>
	''' <remarks></remarks>
	Public Function AddPSM(ByVal peptide As String, ByVal protein As String, ByVal specEValue As Double, ByVal dataLine As String) As Boolean

		Dim udtPSM As udtPSMType

		peptide = RemovePrefixAndSuffix(peptide)

		udtPSM.Peptide = peptide
		udtPSM.SpecEValue = specEValue
		udtPSM.DataLine = dataLine

		Dim updatePeptideDictionary = False

		If mPeptides.Count < mMaximumPSMsToKeep OrElse mPeptides.ContainsKey(peptide) Then

			Dim proteinPeptide = protein & "_" & peptide
			Dim udtExistingPSM As udtPSMType = Nothing

			If mPSMs.TryGetValue(proteinPeptide, udtExistingPSM) Then
				If udtExistingPSM.SpecEValue > specEValue Then
					udtExistingPSM.SpecEValue = specEValue
					mPSMs(proteinPeptide) = udtExistingPSM
				End If
			Else
				mPSMs.Add(proteinPeptide, udtPSM)
			End If

			updatePeptideDictionary = True

		Else

			If specEValue < mWorstSpecEValue Then

				' Remove all entries in mPSMs for the worst scoring peptidein mPSMs
				Dim peptideToRemove = (From item In mPeptides Order By item.Value Descending Take 1 Select item.Key).First()
				
				Dim keysToRemove = (From item In mPSMs Where item.Value.Peptide = peptideToRemove Select item.Key).ToList()

				For Each proteinPeptideKey In keysToRemove
					mPSMs.Remove(proteinPeptideKey)
				Next
				 
				mPeptides.Remove(peptideToRemove)

				' Add the new PSM
				Dim proteinPeptide = protein & "_" & peptide
				mPSMs.Add(proteinPeptide, udtPSM)
				
				updatePeptideDictionary = True


			End If
		End If

		If updatePeptideDictionary Then

			Dim lowestSpecEValue As Double
			If mPeptides.TryGetValue(peptide, lowestSpecEValue) Then
				If specEValue < lowestSpecEValue Then
					mPeptides(peptide) = specEValue				
				End If
			Else
				mPeptides.Add(peptide, specEValue)
			End If

			Dim worstSpecEValueNew = (From item In mPeptides Order By item.Value Descending Take 1 Select item.Value).First()
			mWorstSpecEValue = worstSpecEValueNew

			Return True

		Else
			Return False
		End If


	End Function

	Public Shared Function RemovePrefixAndSuffix(ByVal peptide As String) As String

		If peptide.Length > 4 Then
			If peptide.Chars(1) = "." Then
				peptide = peptide.Substring(2)
			End If
			If peptide.Chars(peptide.Length - 2) = "." Then
				peptide = peptide.Substring(0, peptide.Length - 2)
			End If
		End If

		Return peptide
	End Function

End Class
