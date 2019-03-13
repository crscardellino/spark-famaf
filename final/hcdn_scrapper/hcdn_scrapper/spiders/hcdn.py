# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import itertools
import re
import scrapy
from datetime import datetime
from functools import reduce


class HcdnSpider(scrapy.Spider):
    name = "hcdn"
    allowed_domains = ["http://www.hcdn.gob.ar/proyectos/proyectoTP.jsp?exp="]

    def start_requests(self):
        projects = list()
        projects.append(("%04d-D-2011" % pid for pid in range(1, 6483)))
        projects.append(("%04d-D-2012" % pid for pid in range(1, 8781)))
        projects.append(("%04d-D-2013" % pid for pid in range(1, 8283)))
        projects.append(("%04d-D-2014" % pid for pid in range(1, 10017)))
        projects.append(("%04d-D-2015" % pid for pid in range(3, 6603)))
        projects.append(("%04d-D-2016" % pid for pid in range(2, 7918)))

        for project in itertools.chain(*projects):
            yield scrapy.Request(url="%s%s" % (self.allowed_domains[0], project), callback=self.parse)

    def parse(self, response):
        div = response.css('div.container.tabGral')

        project_type = div.css('h3::text').extract_first().lower().split()[-1]

        if project_type == 'ley':
            id, summary, date = div.css('h3').xpath('./parent::div/text()[normalize-space()]').extract()

            id = id.strip()
            summary = summary.strip()
            date = datetime.strptime(date.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")

            law_text = div.css('#proyectosTexto').\
                xpath(".//div[following-sibling::p and not(contains(@style, 'font-style:italic'))]/text()").extract()
            law_text = reduce(lambda x, y: x + ' ' + y, law_text, '')
            law_text = re.sub(r"\s+", " ", law_text)

            signers = list()
            for tr in div.css('#proyectosFirmantes').xpath('.//tr[child::td]'):
                congressman, district, party = tr.xpath('.//td/text()').extract()
                signers.append(dict(
                    congressman=congressman.strip(),
                    district=district.strip(),
                    party=party.strip()
                ))

            dcommissions = div.css('#proyectosTramite')\
                .xpath('.//table[preceding-sibling::div//text()=" Giro a comisiones en Diputados "]')

            if len(dcommissions) > 0:
                dcommissions = dcommissions[0].xpath('.//td/text()').extract()
            else:
                dcommissions = []

            scommissions = div.css('#proyectosTramite') \
                .xpath('.//table[preceding-sibling::div//text()=" Giro a comisiones en Senado "]')

            if len(scommissions) > 0:
                scommissions = scommissions[0].xpath('.//td/text()').extract()
            else:
                scommissions = []

            results = []

            for row in div.css('#proyectosTramite').xpath('.//table[preceding-sibling::div//text()=" Trámite "]')\
                    .xpath('.//tr[td]'):
                row = row.xpath('.//td/text()').extract()
                if len(row) == 4:
                    chamber, _, rdate, result = row
                    results.append(dict(
                        chamber=chamber,
                        result=result,
                        date=datetime.strptime(rdate.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
                    ))

            yield dict(
                id=id,
                summary=summary,
                date=date,
                law_text=law_text,
                signers=signers,
                dcommissions=dcommissions,
                scommissions=scommissions,
                results=results
            )
