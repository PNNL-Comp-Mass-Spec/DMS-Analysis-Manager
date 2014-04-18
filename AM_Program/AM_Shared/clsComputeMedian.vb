Public Class clsComputeMedian

	Dim mRandom As Random

	Public Sub New()
		mRandom = New Random
	End Sub

	''' <summary>
	''' Partitions the given list around a pivot element such that all elements on left of pivot are less than or equal to pivot
	''' and the ones at thr right are greater than pivot. This method can be used for sorting, N-order statistics such as
	''' as median finding algorithms.
	''' Pivot is selected ranodmly if random number generator is supplied else its selected as last element in the list.
	''' Reference: Introduction to Algorithms 3rd Edition, Corman et al, pp 171
	''' </summary>	
	Private Function Partition(ByVal lstData As IList(Of Double), ByVal startIndex As Integer, ByVal endIndex As Integer, Optional oRandom As Random = Nothing) As Integer
		If oRandom IsNot Nothing Then
			Swap(lstData, endIndex, oRandom.Next(startIndex, endIndex))
		End If

		Dim pivot = lstData(endIndex)
		Dim lastLow = startIndex - 1
		For i As Integer = startIndex To endIndex - 1
			If lstData(i).CompareTo(pivot) <= 0 Then
				lastLow += 1
				Swap(lstData, i, lastLow)
			End If
		Next
		lastLow += 1
		Swap(lstData, endIndex, lastLow)
		Return lastLow

	End Function

	''' <summary>
	''' Returns Nth smallest element from the list. Here n starts from 0 so that n=0 returns minimum, n=1 returns 2nd smallest element etc.
	''' Note: specified list will be mutated in the process.
	''' Reference: Introduction to Algorithms 3rd Edition, Corman et al, pp 216
	''' </summary>
	Public Function NthOrderStatistic(ByVal lstData As IList(Of Double), n As Integer) As Double
		Return NthOrderStatistic(lstData, n, 0, lstData.Count - 1, mRandom)
	End Function

	''' <summary>
	''' Returns Nth smallest element from the list. Here n starts from 0 so that n=0 returns minimum, n=1 returns 2nd smallest element etc.
	''' Note: specified list will be mutated in the process.
	''' Reference: Introduction to Algorithms 3rd Edition, Corman et al, pp 216
	''' </summary>
	Public Function NthOrderStatistic(ByVal lstData As IList(Of Double), n As Integer, oRandom As Random) As Double
		Return NthOrderStatistic(lstData, n, 0, lstData.Count - 1, oRandom)
	End Function

	Private Function NthOrderStatistic(ByVal lstData As IList(Of Double), n As Integer, startIndex As Integer, endIndex As Integer, oRandom As Random) As Double
		While True
			Dim pivotIndex = Partition(lstData, startIndex, endIndex, oRandom)
			If pivotIndex = n Then
				Return lstData(pivotIndex)
			End If

			If n < pivotIndex Then
				endIndex = pivotIndex - 1
			Else
				startIndex = pivotIndex + 1
			End If
		End While

		Throw New Exception("This code should not be reached")
	End Function

	Public Sub Swap(ByVal lstData As IList(Of Double), ByVal i As Integer, ByVal j As Integer)
		If i = j Then
			'This check is not required but Partition function may make many calls so its for perf reason
			Return
		End If
		Dim temp = lstData(i)
		lstData(i) = lstData(j)
		lstData(j) = temp
	End Sub

	''' <summary>
	''' Note: specified list will be mutated in the process.
	''' </summary>
	Public Function Median(ByVal lstData As IList(Of Double)) As Double
		Dim midPoint = CInt((lstData.Count - 1) / 2)
		Return NthOrderStatistic(lstData, midPoint)
	End Function

	Public Function Median(ByVal sequence As IEnumerable(Of Double), getValue As Func(Of Double, Double)) As Double
		Dim lstData = sequence.Select(getValue).ToList()
		Dim midPoint = CInt((lstData.Count - 1) / 2)
		Return NthOrderStatistic(lstData, midPoint)
	End Function

End Class
