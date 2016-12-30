using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TermIndexer
{
    public class Repository
    {
        TermIndexerDBDataContext db = new TermIndexerDBDataContext();

        public void InsertWordsToDB(string word)
        {
                tblTerm term = new tblTerm();
                term.term = word;
                db.tblTerms.InsertOnSubmit(term);
                db.SubmitChanges();
        }
    }
}
