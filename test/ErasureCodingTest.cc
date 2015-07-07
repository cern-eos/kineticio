#include "catch.hpp"
#include "ErasureCoding.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;

using namespace kio;

const char * story[] = {
"0 Eine entscheidende Sitzung gibt es noch: Im Schuldenstreit zwischen ",
"1 Griechenland und seinen Geldgebern kommt nun alles auf das Treffen  ",
"2 der Finanzminister der Eurozone am Samstag an. So haben es Angela   ",
"3 Merkel und andere Staats- und Regierungschefs nach dem EU-Gipfel in ",
"4 der Nacht auf Freitag verkündet. Sie selbst wolle nicht über Einze",
"5 heiten entscheiden, das stellte die Kanzlerin vor den Journalisten  ",
"6 in Brüssel ungewöhnlich deutlich klar. Bereits dieser Aspekt ist  ",
"7 eine deutliche Botschaft in Richtung der griechischen Regierung.    ",
"8 Ministerpräsident Alexis Tsipras hat in den vergangenen Monaten den",
"9 Schuldenstreit stets als politische Auseinandersetzung .            "
};

SCENARIO("Erasure Encoding Test.", "[Erasure]"){

  GIVEN ("An Erasure Code"){
    int nData = 10;
    int nParity = 3;
    ErasureCoding e(nData, nParity);

    std::vector<shared_ptr<const string> > stripe(nData+nParity);
      for(int i=0; i<nData; i++)
        stripe[i] = make_shared<const string>((char*)story[i]);

    WHEN("We encoded Parity Information. "){

      REQUIRE_NOTHROW(e.compute(stripe));

      THEN("We can reconstruct randomly deleted lines."){
        srand (time(NULL));
        for(int i=0; i<nParity; i++){
          auto indx = rand()%(nData+nParity);
          stripe[indx] = make_shared<const string>();
        }
        REQUIRE_NOTHROW(e.compute(stripe));
        for(int i=0; i<nData; i++){
          REQUIRE(stripe[i]->compare(story[i])==0);
        }
      }
    }

    THEN("Too view healthy chunks throws."){
      stripe[0] = make_shared<const string>();
      REQUIRE_THROWS(e.compute(stripe));
    }

    THEN("Invalid chunk size throws."){
      std::string s = * stripe[0];
      s.append("This Chunk is too long.");
      stripe[0] = make_shared<const string>(s);
      REQUIRE_THROWS(e.compute(stripe));
    }

    THEN("Invalid stripe size throws."){
      stripe.pop_back();
      REQUIRE_THROWS(e.compute(stripe));
    }

  }
};