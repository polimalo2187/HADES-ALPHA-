"""
Servicios de Negocio - Scanner Avanzado
Escaneo de mercado en tiempo real con detección de oportunidades
"""
import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)

class Opportunity:
    """Modelo de oportunidad de trading"""
    def __init__(self, type: str, symbol: str, **kwargs):
        self.type = type
        self.symbol = symbol
        self.exchange = kwargs.get('exchange')
        self.buy_exchange = kwargs.get('buy_exchange')
        self.sell_exchange = kwargs.get('sell_exchange')
        self.buy_price = kwargs.get('buy_price', 0)
        self.sell_price = kwargs.get('sell_price', 0)
        self.current_price = kwargs.get('current_price', 0)
        self.spread_pct = kwargs.get('spread_pct', 0)
        self.confidence = kwargs.get('confidence', 0)
        self.metadata = kwargs.get('metadata', {})
        
    def model_dump(self) -> Dict:
        return {
            "type": self.type,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "buy_exchange": self.buy_exchange,
            "sell_exchange": self.sell_exchange,
            "buy_price": float(self.buy_price) if self.buy_price else 0,
            "sell_price": float(self.sell_price) if self.sell_price else 0,
            "current_price": float(self.current_price) if self.current_price else 0,
            "spread_pct": round(float(self.spread_pct), 4),
            "confidence": round(float(self.confidence), 2),
            "metadata": self.metadata
        }

class AdvancedScannerService:
    """
    Servicio de escaneo avanzado de mercado
    Detecta oportunidades basadas en spreads, volumen y volatilidad
    """
    
    def __init__(self):
        self.db = None
        self.is_running = False
        self.scan_interval = 5  # segundos
        self.opportunities_found = 0
        
    def set_database(self, db):
        """Establecer conexión a la base de datos"""
        self.db = db
        
    async def start(self):
        """Iniciar el servicio de escaneo en background"""
        self.is_running = True
        logger.info("📡 Advanced Scanner Service started")
        asyncio.create_task(self._scan_loop())
        
    async def stop(self):
        """Detener el servicio de escaneo"""
        self.is_running = False
        logger.info("🛑 Advanced Scanner Service stopped")
        
    async def _scan_loop(self):
        """Bucle principal de escaneo"""
        while self.is_running:
            try:
                await self._scan_market()
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                logger.error(f"❌ Error in scan loop: {e}")
                await asyncio.sleep(self.scan_interval)
    
    async def _scan_market(self):
        """Ejecutar escaneo de mercado completo"""
        if not self.db:
            return
            
        now = datetime.utcnow()
        
        # Obtener precios actualizados de la cache/DB
        prices = await self._get_latest_prices()
        if not prices:
            return
            
        # Detectar oportunidades de arbitraje
        arb_opps = await self._detect_arbitrage(prices)
        
        # Detectar movimientos bruscos de volumen
        volume_opps = await self._detect_volume_spikes(prices)
        
        # Detectar divergencias entre exchanges
        spread_opps = await self._detect_high_spreads(prices)
        
        # Guardar oportunidades en DB
        all_opps = arb_opps + volume_opps + spread_opps
        if all_opps:
            await self._save_opportunities(all_opps, now)
            self.opportunities_found += len(all_opps)
            
    async def _get_latest_prices(self) -> Dict:
        """Obtener últimos precios desde la colección de market data"""
        collection = self.db.market_data
        # Obtener últimos datos por símbolo
        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$symbol",
                "latest": {"$first": "$$ROOT"}
            }}
        ]
        results = await collection.aggregate(pipeline).to_list(length=100)
        return {item["_id"]: item["latest"] for item in results} if results else {}
    
    async def _detect_arbitrage(self, prices: Dict) -> List[Opportunity]:
        """Detectar oportunidades de arbitraje entre exchanges"""
        opportunities = []
        
        # Agrupar por símbolo
        by_symbol = {}
        for symbol, data in prices.items():
            if symbol not in by_symbol:
                by_symbol[symbol] = []
            by_symbol[symbol].append(data)
        
        # Buscar diferencias de precio > 0.5%
        for symbol, exchange_data in by_symbol.items():
            if len(exchange_data) < 2:
                continue
                
            sorted_data = sorted(exchange_data, key=lambda x: float(x.get('price', 0)))
            lowest = sorted_data[0]
            highest = sorted_data[-1]
            
            low_price = float(lowest.get('price', 0))
            high_price = float(highest.get('price', 0))
            
            if low_price == 0:
                continue
                
            spread_pct = ((high_price - low_price) / low_price) * 100
            
            if spread_pct > 0.5:  # Umbral mínimo 0.5%
                opp = Opportunity(
                    type="arbitrage",
                    symbol=symbol,
                    buy_exchange=lowest.get('exchange'),
                    sell_exchange=highest.get('exchange'),
                    buy_price=low_price,
                    sell_price=high_price,
                    spread_pct=spread_pct,
                    confidence=min(95, 50 + spread_pct * 10),
                    metadata={
                        "volume_buy": lowest.get('volume_24h', 0),
                        "volume_sell": highest.get('volume_24h', 0)
                    }
                )
                opportunities.append(opp)
                
        return opportunities
    
    async def _detect_volume_spikes(self, prices: Dict) -> List[Opportunity]:
        """Detectar picos de volumen inusuales"""
        opportunities = []
        
        for symbol, data in prices.items():
            current_vol = float(data.get('volume_24h', 0))
            avg_vol = float(data.get('avg_volume_7d', current_vol))
            
            if avg_vol == 0:
                continue
                
            vol_ratio = current_vol / avg_vol
            
            if vol_ratio > 2.0:  # Volumen 2x superior al promedio
                opp = Opportunity(
                    type="volume_spike",
                    symbol=symbol,
                    exchange=data.get('exchange'),
                    current_price=float(data.get('price', 0)),
                    spread_pct=0,
                    confidence=min(90, 60 + (vol_ratio - 2) * 10),
                    metadata={
                        "current_volume": current_vol,
                        "avg_volume": avg_vol,
                        "ratio": round(vol_ratio, 2)
                    }
                )
                opportunities.append(opp)
                
        return opportunities
    
    async def _detect_high_spreads(self, prices: Dict) -> List[Opportunity]:
        """Detectar spreads altos dentro del mismo exchange"""
        # Implementación para futuros: detectar bid-ask spreads anormales
        return []
    
    async def _save_opportunities(self, opportunities: List[Opportunity], timestamp: datetime):
        """Guardar oportunidades en la base de datos"""
        collection = self.db.opportunities
        
        docs = []
        for opp in opportunities:
            doc = opp.model_dump()
            doc['detected_at'] = timestamp
            doc['status'] = 'new'
            docs.append(doc)
            
        if docs:
            await collection.insert_many(docs)
            logger.info(f"💾 Saved {len(docs)} opportunities to DB")
            
    def get_stats(self) -> Dict:
        """Obtener estadísticas del scanner"""
        return {
            "is_running": self.is_running,
            "scan_interval": self.scan_interval,
            "opportunities_found": self.opportunities_found,
            "last_scan": datetime.utcnow().isoformat()
        }

# Instancia global
scanner_service = AdvancedScannerService()
