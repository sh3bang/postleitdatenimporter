use std::collections::{HashMap};

use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;

use std::sync::Arc;
use std::time::Instant;

use elasticsearch::http::Url;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use serde_json::{json,Value};
use elasticsearch::{Elasticsearch,BulkParts};
use elasticsearch::http::{request::JsonBody};


/// Decoding table (CP850 to Unicode)
static DECODING_TABLE_CP850: [char; 128] = [
    '\u{00C7}', '\u{00FC}', '\u{00E9}', '\u{00E2}', '\u{00E4}', '\u{00E0}', '\u{00E5}', '\u{00E7}',
    '\u{00EA}', '\u{00EB}', '\u{00E8}', '\u{00EF}', '\u{00EE}', '\u{00EC}', '\u{00C4}', '\u{00C5}',
    '\u{00C9}', '\u{00E6}', '\u{00C6}', '\u{00F4}', '\u{00F6}', '\u{00F2}', '\u{00FB}', '\u{00F9}',
    '\u{00FF}', '\u{00D6}', '\u{00DC}', '\u{00F8}', '\u{00A3}', '\u{00D8}', '\u{00D7}', '\u{0192}',
    '\u{00E1}', '\u{00ED}', '\u{00F3}', '\u{00FA}', '\u{00F1}', '\u{00D1}', '\u{00AA}', '\u{00BA}',
    '\u{00BF}', '\u{00AE}', '\u{00AC}', '\u{00BD}', '\u{00BC}', '\u{00A1}', '\u{00AB}', '\u{00BB}',
    '\u{2591}', '\u{2592}', '\u{2593}', '\u{2502}', '\u{2524}', '\u{00C1}', '\u{00C2}', '\u{00C0}',
    '\u{00A9}', '\u{2563}', '\u{2551}', '\u{2557}', '\u{255D}', '\u{00A2}', '\u{00A5}', '\u{2510}',
    '\u{2514}', '\u{2534}', '\u{252C}', '\u{251C}', '\u{2500}', '\u{253C}', '\u{00E3}', '\u{00C3}',
    '\u{255A}', '\u{2554}', '\u{2569}', '\u{2566}', '\u{2560}', '\u{2550}', '\u{256C}', '\u{00A4}',
    '\u{00F0}', '\u{00D0}', '\u{00CA}', '\u{00CB}', '\u{00C8}', '\u{0131}', '\u{00CD}', '\u{00CE}',
    '\u{00CF}', '\u{2518}', '\u{250C}', '\u{2588}', '\u{2584}', '\u{00A6}', '\u{00CC}', '\u{2580}',
    '\u{00D3}', '\u{00DF}', '\u{00D4}', '\u{00D2}', '\u{00F5}', '\u{00D5}', '\u{00B5}', '\u{00FE}',
    '\u{00DE}', '\u{00DA}', '\u{00DB}', '\u{00D9}', '\u{00FD}', '\u{00DD}', '\u{00AF}', '\u{00B4}',
    '\u{00AD}', '\u{00B1}', '\u{2017}', '\u{00BE}', '\u{00B6}', '\u{00A7}', '\u{00F7}', '\u{00B8}',
    '\u{00B0}', '\u{00A8}', '\u{00B7}', '\u{00B9}', '\u{00B3}', '\u{00B2}', '\u{25A0}', '\u{00A0}',
];

trait DecodeCp850 {
    fn from_cp850(v: &[u8]) -> String {
        let field = match String::from_utf8(v.to_vec()) {
            Ok(passthrough) => passthrough,
            Err(failed) => {
                let mut decoded = String::with_capacity(v.len()*3 as usize);
                failed.as_bytes().iter().for_each(|byte| {
                    if *byte < 128 {
                        decoded.push(*byte as char);
                    } else {        
                        decoded.push(DECODING_TABLE_CP850[(*byte & 127) as usize]);
                    }
                });
                decoded
            }
        };
        field.trim().to_string()
    }
}

impl DecodeCp850 for String {}

// Kreisgemeindedatei KGS-DA
struct KgsDa {
    kg_schluessel: String,
    kg_satzart: String,
    kg_name: String,
}

// Orte- und Ortsarchivdatei ORT-DA (Seite 12)
struct OrtDa {
    ort_alort: String,
    ort_status: String,
    ort_oname: String,
    ort_ozusatz: String,
}

// Ortsteiledatei OTL-DB (Seite 14)
struct OtlDb {
    otl_alort: String,
    otl_schl: String,
    otl_plz: String,
    otl_status: String,
    otl_name: String,
}

// Straßen- und Straßenarchivdatei STRA-DB (Handbuch Seite 18)
#[derive(Clone)]
struct StraDb {
    str_alort: String,
    str_name46: String,
    str_plz: String,
    str_otl_schl: String,
    str_kgs: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
//fn main() -> Result<(), Box<dyn std::error::Error>> {

    let start = Instant::now();

    let file = File::open("/home/paul/Downloads/B2210277.DAT")?;
    let line_length = 326;
    
    
    let mut reader = BufReader::new(file);

    let mut buf = Vec::with_capacity(line_length as usize);

    let mut eol: i64 = 0;

    let mut kgs_da = HashMap::with_capacity(12000);
    let mut ort_da = HashMap::with_capacity(77000);
    let mut otl_db = HashMap::with_capacity(13200);
    let mut stra_db = Vec::with_capacity(1300000);

    while reader.read_until(b'$', &mut buf)? > 0 {

        match &buf[..2] {
            br#"KG"# => { // Kreisgemeindedatei KGS-DA (Handbuch Seite 9)
                let v = KgsDa {
                    kg_schluessel: String::from_cp850(&buf[17..=24]),
                    kg_satzart: String::from_cp850(&buf[25..=25]),
                    kg_name: String::from_cp850(&buf[26..=65]),
                };
                let k = format!("{}{}", // Primärschlüssel (im Handbuch Grau hinterlegt)
                    v.kg_schluessel,
                    v.kg_satzart
                );
                kgs_da.insert(k, v);
            },
            br#"OR"# => { // Orte- und Ortsarchivdatei ORT-DA (Handbuch Seite 12)
                let ort_status = &buf[25..=25];
                if ort_status == b"G".as_slice() {
                    let v = OrtDa {
                        ort_alort: String::from_cp850(&buf[17..=24]),
                        ort_status: String::from_cp850(ort_status),
                        ort_oname: String::from_cp850(&buf[26..=65]),
                        ort_ozusatz: String::from_cp850(&buf[106..=135]),
                    };
                    let k = format!("{}{}", // Primärschlüssel (im Handbuch Grau hinterlegt)
                        v.ort_alort,
                        v.ort_status,
                    );
                    ort_da.insert(k, v);
                }
            }
            br#"OB"# => { // Ortsteiledatei OTL-DB (Handbuch Seite 14)
                let otl_status = &buf[33..=33];
                if otl_status == b"G".as_slice() {
                    let v = OtlDb {
                        otl_alort: String::from_cp850(&buf[17..=24]),
                        otl_schl: String::from_cp850(&buf[25..=27]),
                        otl_plz: String::from_cp850(&buf[28..=32]),
                        otl_status: String::from_cp850(otl_status),
                        otl_name: String::from_cp850(&buf[35..=74]),
                    };
                    let k = format!("{}{}{}{}", // Primärschlüssel (im Handbuch Grau hinterlegt)
                        v.otl_alort,
                        v.otl_schl,
                        v.otl_plz,
                        v.otl_status,
                    );
                    otl_db.insert(k, v);
                }
            },
            br#"SB"# => { // Straßen- und Straßenarchivdatei STRA-DB (Handbuch Seite 18)
                let str_status = &buf[52..=52];
                if str_status == b"G".as_slice() {
                    let v = StraDb {
                        str_alort: String::from_cp850(&buf[17..=24]),
                        str_name46: String::from_cp850(&buf[101..=146]),
                        str_plz: String::from_cp850(&buf[171..=175]),
                        str_otl_schl: String::from_cp850(&buf[179..=181]),
                        str_kgs: String::from_cp850(&buf[190..=197])
                    };
                    stra_db.push(v);
                }
            }
            _ => ()
        }

        eol += line_length;
        let pos = reader.stream_position()? as i64;
        let next = eol - pos;
        reader.seek_relative(next)?;

        buf.clear();
    
    }


    
    let url = Url::parse("http://localhost:9200")?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    let client_arc = Arc::new(client);

    let otl_db_arc = Arc::new(otl_db);
    let ort_da_arc = Arc::new(ort_da);
    let kgs_da_arc = Arc::new(kgs_da);



    let chunk_size = 5000;
    let mut at = stra_db.len();
    

    loop {

        if at > chunk_size {
            at -= chunk_size;
        } else if at == 0 {
            break;
        } else {
            at = 0;
        }
        
        
        let chunk = stra_db.split_off(at);
        
        let client_arc = Arc::clone(&client_arc);

        let otl_db_arc = Arc::clone(&otl_db_arc);
        let ort_da_arc = Arc::clone(&ort_da_arc);
        let kgs_da_arc = Arc::clone(&kgs_da_arc);

        tokio::spawn( async move {

            
            let mut bulk: Vec<JsonBody<_>> = Vec::new();         
            let mut f_iter = chunk.iter();
            
            while let Some(f) = f_iter.next() {

                let rel_otl_db = {
                    if f.str_otl_schl.is_empty() {
                        otl_db_arc.get(&format!("{}{}{}G", f.str_alort, f.str_otl_schl, f.str_plz)) // G = gültiger Ortsteil
                    } else {
                        None
                    }
                };

                let rel_ort_da = {
                    if f.str_otl_schl.is_empty() {
                        ort_da_arc.get(&format!("{}G",f.str_alort)) // G = gültiger Orts-Satz
                    } else {
                        None
                    }
                };

                // Nach rechts mit Nullen auffüllen (Achstelliger Schlüssel) + einstellige Satzart:
                // https://doc.rust-lang.org/std/fmt/#fillalignment
                let kgs = &f.str_kgs;
                let kgs_g = format!("{}G", kgs);            // G = Gemeinde (alle 8 Stellen)
                let kgs_k = format!("{:0<8}K", &kgs[0..5]); // K = Landkreis (erste 5 Stellen)
                let kgs_r = format!("{:0<8}R", &kgs[0..3]); // R = Reg.-Bezirk (erste 3 Stellen)
                let kgs_l = format!("{:0<8}L", &kgs[0..2]); // L = Bundesland (erste 2 Stellen)


                bulk.push(json!({"index": {}}).into());
                bulk.push(json!({
                    "street": f.str_name46.clone(),
                    "postalCode": f.str_plz.clone(),
                    "city": match rel_ort_da {
                        Some(v) => Value::String(v.ort_oname.clone()),
                        None => Value::Null
                    },
                    "cityAddition": match rel_ort_da {
                        Some(v) => Value::String(v.ort_ozusatz.clone()),
                        None => Value::Null
                    },
                    "region": match rel_otl_db {
                        Some(v) => Value::String(v.otl_name.clone()),
                        None => Value::Null
                    },
                    "community": match kgs_da_arc.get(&kgs_g) {
                        Some(v) => Value::String(v.kg_name.clone()),
                        None => Value::Null
                    },
                    "country": match kgs_da_arc.get(&kgs_k) {
                        Some(v) => Value::String(v.kg_name.clone()),
                        None => Value::Null
                    },
                    "district": match kgs_da_arc.get(&kgs_r) {
                        Some(v) => Value::String(v.kg_name.clone()),
                        None => Value::Null
                    },
                    "state": match kgs_da_arc.get(&kgs_l) {
                        Some(v) => Value::String(v.kg_name.clone()),
                        None => Value::Null
                    },
                }).into());
            };
            
            let response = client_arc
            .bulk(BulkParts::Index("addresses"))
            .body(bulk)
            .send()
            .await
            .unwrap();
            
            let response_body = response.json::<Value>().await.unwrap();
            println!("{}",response_body["took"]);
            //let _successful = response_body["errors"].as_bool().unwrap() == false;
        })
        .await
        .unwrap();
        

    };



    let elapsed = start.elapsed();
    println!("Dauer: {} ms", elapsed.as_millis());

    Ok(())
}