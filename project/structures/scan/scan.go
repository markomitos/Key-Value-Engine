package scan

import (
	. "project/keyvalue/structures/dataType"
)

// FoundResults -> trenutan broj pronadjenih poklapanja uslova
// SelectedPageStart -> od kog pronadjenog rezultata treba da belezi
// SelectedPageEnd -> do kog pronadjenog rezultata treba da belezi
// Keys -> niz kljuceva koji cemo azurirati kako god pronadjemo rezultat u opsegu
// Data -> niz podataka koji cemo azurirati kako god pronadjemo rezultat u opsegu

//Struktura koja predstavlja trenutnu potragu za stranicom
type Scan struct {
	FoundResults uint32
	SelectedPageStart uint32
	SelectedPageEnd uint32
	Keys []string
	Data []*Data
	RemovedKeys map[string]bool
	SelectedKeys map[string]bool
}

func NewScan(pageLen uint32, pageNum uint32) *Scan{
	scan := new(Scan)
	scan.FoundResults = 0
	scan.Data = make([]*Data, 0)
	scan.Keys = make([]string, 0)

	//Kako prolazimo kroz podatke cim naidjemo na neki obrisan dodajemo ga ovde i necemo ga uzimati u obzir na dalje
	scan.RemovedKeys = make(map[string]bool, 0) 

	//Kako prolazimo kroz podatke cim dodamo neki element ovde belezimo
	//proveravamo svaki naredni put kako ne bi dodali stariju verziju elementa
	scan.SelectedKeys = make(map[string]bool, 0) 

	scan.SelectedPageStart = (pageNum-1)*pageLen+1
	scan.SelectedPageEnd = (pageNum)*pageLen
	return scan
}