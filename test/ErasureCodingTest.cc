#include "catch.hpp"
#include "ErasureCoding.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;

using namespace kio;

std::string value(
"0 Eine entscheidende Sitzung gibt es noch: Im Schuldenstreit zwischen "
"1 Griechenland und seinen Geldgebern kommt nun alles auf das Treffen "
"2 der Finanzminister der Eurozone am Samstag an. So haben es Angela "
"3 Merkel und andere Staats- und Regierungschefs nach dem EU-Gipfel in "
"4 der Nacht auf Freitag verkündet. Sie selbst wolle nicht über Einzelheiten "
"5 heiten entscheiden, das stellte die Kanzlerin vor den Journalisten  "
"6 in Brüssel ungewöhnlich deutlich klar. Bereits dieser Aspekt ist "
"7 eine deutliche Botschaft in Richtung der griechischen Regierung. "
"8 Ministerpräsident Alexis Tsipras hat in den vergangenen Monaten den"
"9 Schuldenstreit stets als politische Auseinandersetzung. "
);

SCENARIO("Erasure Encoding Test.", "[Erasure]"){

  GIVEN ("An Erasure Code"){
    int nData = 10;
    int nParity = 3;
    ErasureCoding e(nData, nParity);

    size_t chunk_size = (value.size() + nData-1) / (nData);
    std::vector< shared_ptr<const string> > stripe;
    for(int i=0; i<nData+nParity; i++){
      if(i<nData){
        auto subchunk = make_shared<string>(value.substr(i*chunk_size, chunk_size));
        subchunk->resize(chunk_size); // ensure that all chunks are the same size
        stripe.push_back(std::move(subchunk));
      }
      else
        stripe.push_back(make_shared<string>());
    }

    WHEN("We encoded Parity Information. "){

      REQUIRE_NOTHROW(e.compute(stripe));

      THEN("We can reconstruct randomly deleted lines."){
        srand (time(NULL));
        for(int i=0; i<nParity; i++){
          auto indx = rand()%(nData+nParity);
          printf("Removing Chunk %d: %s\n",indx,stripe[indx] ? stripe[indx]->c_str() : "");
          stripe[indx] = make_shared<const string>();
        }
        REQUIRE_NOTHROW(e.compute(stripe));

        std::string reconstructed;
        for(int i=0; i<nData; i++){
          reconstructed.append(stripe[i]->c_str());
        }
        reconstructed.resize(value.size());
        REQUIRE(value == reconstructed);
        printf("Reconstructed Story: %s\n",reconstructed.c_str());
      }
    }

    THEN("Too few healthy chunks throws."){
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