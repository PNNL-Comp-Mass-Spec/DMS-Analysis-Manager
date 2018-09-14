using PdfSharp;
using PdfSharp.Drawing;
using PdfSharp.Pdf;

namespace AnalysisManagerFormularityPlugin
{
    internal class PdfPageInfo
    {
        /// <summary>
        /// Current vertical offset for the page
        /// </summary>
        public double CurrentPageY { get; private set; }

        /// <summary>
        /// PDF page
        /// </summary>
        public PdfPage Page { get; }

        /// <summary>
        /// Drawing surface (canvas) for the PDF page
        /// </summary>
        public XGraphics PageGraphics { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pdfDoc"></param>
        /// <param name="pageMargin">Page margin, in points</param>
        /// <param name="orientation">Page orientation (portrait or landscape)</param>
        public PdfPageInfo(PdfDocument pdfDoc, double pageMargin, PageOrientation orientation = PageOrientation.Portrait)
        {
            Page = pdfDoc.AddPage();
            Page.Orientation = orientation;

            PageGraphics = XGraphics.FromPdfPage(Page);

            CurrentPageY = pageMargin;
        }

        /// <summary>
        /// Increment CurrentPageY by the given number of points
        /// </summary>
        /// <param name="points"></param>
        public void IncrementY(double points)
        {
            CurrentPageY += points;
        }
    }
}
