using System;
using System.Collections.Generic;
using System.IO;
using PdfSharp.Drawing;
using PdfSharp.Pdf;

namespace AnalysisManagerFormularityPlugin
{
    public class PngToPdfConverter : PRISM.clsEventNotifier
    {
        /// <summary>
        /// Page margin, in points
        /// </summary>
        /// <remarks>
        /// For reference: 72 points is 1 inch, as defined by the DTP (or PostScript) point.
        /// </remarks>
        private const double PageMargin = 36;

        private const double DoublePageMargin = PageMargin * 2;

        private readonly XFont mFontHeader;

        private readonly XFont mFontDefault;

        /// <summary>
        /// Dataset name
        /// </summary>
        private string DatasetName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public PngToPdfConverter(string datasetName)
        {
            DatasetName = datasetName;

            const string FONT_FAMILY = "Arial";
            mFontHeader = new XFont(FONT_FAMILY, 20, XFontStyle.Regular);
            mFontDefault = new XFont(FONT_FAMILY, 10, XFontStyle.Regular);
        }

        /// <summary>
        /// Add a new page with landscape orientation
        /// </summary>
        /// <param name="pdfDoc"></param>
        /// <returns></returns>
        private PdfPageInfo AddPage(PdfDocument pdfDoc)
        {
            var currentPageInfo = new PdfPageInfo(pdfDoc, PageMargin, PageOrientation.Landscape);
            return currentPageInfo;
        }

        /// <summary>
        /// Adds a plot image
        /// </summary>
        /// <param name="currentPage">Current PDF page</param>
        /// <param name="plotImage">Plot image</param>
        /// <param name="xOffset">X offset</param>
        /// <param name="width">Plot width, in points</param>
        /// <param name="height">Plot height, in points</param>
        /// <param name="header">Header (optional)</param>
        /// <returns></returns>
        private void AddPlot(
            PdfPageInfo currentPage, XImage plotImage, double xOffset,
            double width, double height, string header = null)
        {
            var y = currentPage.CurrentPageY;
            if (!string.IsNullOrWhiteSpace(header))
            {
                // Add a header over the image
                var yOffsetIncrement = AddText(currentPage, header, mFontDefault, xOffset: xOffset);
                y += yOffsetIncrement - 10;
            }

            currentPage.PageGraphics.DrawImage(plotImage, new XRect(xOffset, y, width, height));
            plotImage.Dispose();
        }

        /// <summary>
        /// Add text
        /// </summary>
        /// <param name="currentPage">Current page</param>
        /// <param name="text">Text to add</param>
        /// <param name="font">Text font</param>
        /// <param name="yScalar">Y scalar; 0 or 1 means full size text</param>
        /// <param name="xOffset">X offset; if 0, will be at the left page margin</param>
        /// <param name="width">Textbox width; if 0 or larger than the page width, will center the text on the page</param>
        /// <param name="position">Position of the text in the textbox</param>
        /// <returns></returns>
        private double AddText(
            PdfPageInfo currentPage, string text, XFont font,
            double yScalar = 0, double xOffset = 0,
            double width = -1, XStringFormat position = null)
        {
            var textHeight = GetTextHeight(text, currentPage.PageGraphics, font);
            var y = currentPage.CurrentPageY;

            if (Math.Abs(yScalar) > 0)
            {
                y += textHeight * yScalar;
            }

            double x;
            if (xOffset < PageMargin || xOffset > currentPage.Page.Width - PageMargin)
            {
                x = PageMargin;
            }
            else
            {
                x = xOffset;
            }

            if (width < x || width > currentPage.Page.Width - DoublePageMargin)
            {
                width = currentPage.Page.Width - DoublePageMargin;
            }

            if (position == null)
            {
                position = XStringFormats.Default;
            }

            var height = textHeight;
            if (position.LineAlignment == XLineAlignment.BaseLine)
            {
                height = 0;
            }

            currentPage.PageGraphics.DrawString(text, font, XBrushes.Black, new XRect(x, y, width, height), position);

            // Return the effective text height after adjustments
            // This value is used to increment currentPage.CurrentPageY
            return y - currentPage.CurrentPageY + textHeight;
        }

        /// <summary>
        /// Create a PDF file with the given PNG files
        /// </summary>
        /// <param name="pdfFilePath">Path to the PDF file to be created</param>
        /// <param name="pngFiles"></param>
        /// <param name="dataSource"></param>
        /// <returns></returns>
        public bool CreatePdf(string pdfFilePath, List<FileInfo> pngFiles, string dataSource = "NOMSI")
        {
            if (string.IsNullOrWhiteSpace(dataSource))
            {
                dataSource = "Unknown_Source";
            }

            try
            {

                var pdfDoc = new PdfDocument();
                pdfDoc.Options.NoCompression = true;
                pdfDoc.PageMode = PdfPageMode.UseNone;

                // Get the plot layout

                var datasetDetailReportLink = "";
                var pngFileTableLayout = GetPngFileTableLayout(DatasetName, datasetDetailReportLink);

                var pngFileNames = new SortedSet<string>();
                foreach (var item in pngFiles)
                {
                    pngFileNames.Add(item.Name);
                }


                var plotHeight = (mCurrentPage.Width - DoublePageMargin - 10) / 3;

                const int xOffsetIncrement = 300;

                foreach (var tableRow in pngFileTableLayout)
                {
                    var xOffset = PageMargin;
                    double yOffsetIncrement = 0;

                    foreach (var tableCell in tableRow)
                    {
                        if (!string.IsNullOrWhiteSpace(tableCell))
                        {

                            if (pngFileNames.Contains(tableCell))
                            {
                                var pngFilePath = tableCell;

                                // Obsolete: var textHeight = GetTextHeight("int", mCurrentPageGraphics, mFontDefault);

                                var plotImage = XImage.FromFile(pngFilePath);

                                var plotWidth = plotHeight;

                                // Note that AddPlot will call plotImage.Dispose
                                AddPlot(plotImage, xOffset, plotImage.PointHeight, plotImage.PointWidth, "");
                                xOffset += plotWidth + 5;

                                yOffsetIncrement = Math.Max(yOffsetIncrement, plotHeight);
                            }
                            else
                            {
                                var textHeight = AddText(string.Format("File not found: {0}", tableCell), mFontDefault, 0, xOffset);

                                yOffsetIncrement = Math.Max(yOffsetIncrement, textHeight);
                            }
                        }

                        xOffset += xOffsetIncrement;
                    }

                    currentPageY += yOffsetIncrement;
                }

                return true;
            }
            catch (Exception ex)
            {
                base.OnErrorEvent("Error creating the PDF using .png files from " + dataSource, ex);
                return false;
            }

        }

        public static List<List<string>> GetPngFileTableLayout(string datasetName, string datasetDetailReportLink)
        {

            // PNG filename suffix
            var suffix = "_" + datasetName + ".png";

            // This tracks the PNG file names that will be added to the PDF file
            // There are two PNG images in each row
            // Use "text: " to instead include literal text instead of a PNG file
            var tableLayoutByRow = new List<List<string>>
            {
                new List<string> {"EC_count" + suffix, "KMD1_assigned" + suffix},
                new List<string> {"vK" + suffix, "KMD1_unassigned" + suffix},

                new List<string> {"histA" + suffix, "mErr" + suffix},
                new List<string> {"histM" + suffix, datasetDetailReportLink},

                new List<string> {"Ox" + suffix, "OxN" + suffix},
                new List<string> {"OxS" + suffix, "OxP" + suffix}
            };

            return tableLayoutByRow;
        }

        private static double GetTextHeight(string text, XGraphics gfx, XFont font)
        {
            var textSize = gfx.MeasureString(text, font);
            return textSize.Height;
        }
    }
}
