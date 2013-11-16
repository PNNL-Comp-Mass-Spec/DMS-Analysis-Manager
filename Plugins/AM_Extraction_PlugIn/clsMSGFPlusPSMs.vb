Public Class clsMSGFPlusPSMs

	Public Structure udtPSMType
		Public Peptide As String
		Public SpecEValue As Double
		Public DataLine As String
	End Structure

	' Keys are the protein and peptide (separated by an undercore)
	' Values are the PSM details, including the original data line from the .TSV file
	Private mPSMs As Dictionary(Of String, udtPSMType)

	' List of SpecEValues associated with this scan/charge
	Private ReadOnly mSpecEValues As SortedSet(Of Double)

	Private mBestSpecEValue As Double
	Private mWorstSpecEValue As Double

	Private ReadOnly mCharge As Integer
	Private ReadOnly mScan As Integer

	Private ReadOnly mMaximumPSMsToKeep As Integer

	Public ReadOnly Property Charge() As Integer
		Get
			Return mCharge
		End Get
	End Property

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

	Public ReadOnly Property Scan() As Integer
		Get
			Return mScan
		End Get
	End Property

	Public Sub New(ByVal scanNumber As Integer, chargeState As Integer, ByVal maximumPSMsToRetain As Integer)
		mMaximumPSMsToKeep = maximumPSMsToRetain
		If mMaximumPSMsToKeep < 1 Then mMaximumPSMsToKeep = 1

		mPSMs = New Dictionary(Of String, udtPSMType)
		mSpecEValues = New SortedSet(Of Double)

		mBestSpecEValue = 0
		mWorstSpecEValue = 0

		mScan = scanNumber
		mCharge = chargeState
	End Sub

	''' <summary>
	''' Adds the given PSM if the list has fewer than MaximumPSMsToKeep PSMs, or if the specEValue is less than the worst scoring entry in the list
	''' </summary>
	''' <param name="udtPSM"></param>
	''' <param name="protein"></param>
	''' <returns>True if the PSM was stored, otherwise false</returns>
	''' <remarks></remarks>
	Public Function AddPSM(ByVal udtPSM As udtPSMType, ByVal protein As String) As Boolean

		udtPSM.Peptide = RemovePrefixAndSuffix(udtPSM.Peptide)

		Dim updateScores = False
		Dim addPeptide As Boolean = False

		If mSpecEValues.Count < mMaximumPSMsToKeep Then
			addPeptide = True
		Else
			If (From item In mPSMs.Values Where item.Peptide = udtPSM.Peptide Select item).Any() Then
				addPeptide = True
			End If
		End If

		Dim proteinPeptide = protein & "_" & udtPSM.Peptide

		If addPeptide Then

			Dim udtExistingPSM As udtPSMType = Nothing

			If mPSMs.TryGetValue(proteinPeptide, udtExistingPSM) Then
				If udtExistingPSM.SpecEValue > udtPSM.SpecEValue Then
					udtExistingPSM.SpecEValue = udtPSM.SpecEValue
					' Update the dictionary (necessary since udtExistingPSM is a structure and not an object)
					mPSMs(proteinPeptide) = udtExistingPSM
				End If
			Else
				mPSMs.Add(proteinPeptide, udtPSM)
			End If

			updateScores = True

		Else

			If udtPSM.SpecEValue < mWorstSpecEValue Then

				If mPSMs.Count <= 1 OrElse mSpecEValues.Count = 1 Then
					mPSMs.Clear()
				Else

					' Remove all entries in mPSMs for the worst scoring peptide (or tied peptides) in mSpecEValues
					Dim keysToRemove = (From item In mPSMs Where Math.Abs(item.Value.SpecEValue - mWorstSpecEValue) < Double.Epsilon Select item.Key).ToList()

					For Each proteinPeptideKey In keysToRemove.Distinct()
						mPSMs.Remove(proteinPeptideKey)
					Next
				End If

				' Add the new PSM
				mPSMs.Add(proteinPeptide, udtPSM)

				updateScores = True

			ElseIf Math.Abs(udtPSM.SpecEValue - mBestSpecEValue) < Double.Epsilon Then
				' The new peptide has the same score as the best scoring peptide; keep it (and don't remove anything)

				' Add the new PSM
				mPSMs.Add(proteinPeptide, udtPSM)

				updateScores = True

			End If
		End If

		If updateScores Then

			If mPSMs.Count > 1 Then
				' Make sure all peptides have the same SpecEvalue
				Dim bestScoreByPeptide = New Dictionary(Of String, Double)

				For Each psm In mPSMs
					Dim peptideToFind As String = psm.Value.Peptide
					Dim storedScore As Double
					If bestScoreByPeptide.TryGetValue(peptideToFind, storedScore) Then
						bestScoreByPeptide(peptideToFind) = Math.Min(storedScore, psm.Value.SpecEValue)
					Else
						bestScoreByPeptide.Add(peptideToFind, psm.Value.SpecEValue)
					End If
				Next

				For Each key In mPSMs.Keys.ToList()
					Dim udtStoredPSM = mPSMs(key)
					Dim bestScore = bestScoreByPeptide(udtStoredPSM.Peptide)
					If bestScore < udtStoredPSM.SpecEValue Then
						udtStoredPSM.SpecEValue = bestScore
						mPSMs(key) = udtStoredPSM
					End If
				Next
			End If

			' Update the distinct list of SpecEValues
			mSpecEValues.Clear()
			mSpecEValues.UnionWith((From item In mPSMs Select item.Value.SpecEValue).Distinct())

			mBestSpecEValue = mSpecEValues.First
			mWorstSpecEValue = mSpecEValues.Last

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
