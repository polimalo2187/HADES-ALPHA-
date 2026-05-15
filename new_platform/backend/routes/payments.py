"""
Rutas de Pagos - Verificación On-Chain BSC
Sin servicios de terceros, todo interno
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from bson import ObjectId
import aiohttp
from web3 import Web3

from backend.database import get_database
from backend.models import new_payment_order, activate_plan, is_plan_active
from backend.config import settings

router = APIRouter(prefix="/payments", tags=["payments"])


# =========================
# CONFIGURACIÓN WEB3
# =========================
def get_web3() -> Web3:
    """Inicializa conexión con BSC"""
    return Web3(Web3.HTTPProvider(settings.bsc_rpc_http_url))


def verify_bep20_transfer(
    w3: Web3,
    tx_hash: str,
    expected_from: str,
    expected_to: str,
    token_contract: str,
    min_amount: float,
) -> Dict[str, Any]:
    """Verifica una transferencia BEP20 en BSC"""
    try:
        # Obtener transacción
        tx = w3.eth.get_transaction(tx_hash)
        
        # Verificar estado
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        if not tx_receipt or tx_receipt.status != 1:
            return {"valid": False, "reason": "Transaction failed"}
        
        # Verificar confirmaciones
        current_block = w3.eth.block_number
        confirmations = current_block - tx_receipt.blockNumber
        
        # Verificar dirección destino
        if tx.to.lower() != expected_to.lower():
            return {"valid": False, "reason": "Wrong recipient address"}
        
        # Verificar evento Transfer (ERC20/BEP20)
        token_abi = [{
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "from", "type": "address"},
                {"indexed": True, "name": "to", "type": "address"},
                {"indexed": False, "name": "value", "type": "uint256"}
            ],
            "name": "Transfer",
            "type": "event"
        }]
        
        token = w3.eth.contract(address=Web3.to_checksum_address(token_contract), abi=token_abi)
        
        transfer_found = False
        transferred_amount = 0
        
        for log in tx_receipt.logs:
            try:
                event = token.events.Transfer().process_log(log)
                if (
                    event.args.from_.lower() == expected_from.lower() and
                    event.args.to.lower() == expected_to.lower()
                ):
                    transfer_found = True
                    # Convertir de wei a unidades humanas (asumiendo 18 decimales para USDT en BSC)
                    decimals = 18
                    transferred_amount = float(event.args.value) / (10 ** decimals)
                    break
            except:
                continue
        
        if not transfer_found:
            return {"valid": False, "reason": "Transfer event not found"}
        
        if transferred_amount < min_amount:
            return {
                "valid": False, 
                "reason": f"Insufficient amount: {transferred_amount} < {min_amount}"
            }
        
        return {
            "valid": True,
            "confirmations": confirmations,
            "amount": transferred_amount,
            "block_number": tx_receipt.blockNumber,
            "timestamp": datetime.utcfromtimestamp(w3.eth.get_block(tx_receipt.blockNumber).timestamp)
        }
        
    except Exception as e:
        return {"valid": False, "reason": str(e)}


# =========================
# RUTAS DE PAGOS
# =========================

@router.post("/create-order")
async def create_payment_order(
    user_id: int,
    plan: str,
    days: int = 30,
    db=Depends(get_database),
):
    """Crea una orden de pago para suscripción"""
    
    # Precios base en USDT
    PRICES = {
        "free": 0,
        "plus": 29.99,
        "premium": 59.99,
    }
    
    if plan not in PRICES:
        raise HTTPException(status_code=400, detail="Plan inválido")
    
    base_price = PRICES[plan]
    if base_price == 0:
        raise HTTPException(status_code=400, detail="El plan free no requiere pago")
    
    # Calcular precio proporcional por días
    amount_usdt = base_price * (days / 30)
    
    # Generar ID único
    order_id = f"ORD-{datetime.utcnow().timestamp()}-{user_id}"
    
    # Dirección de depósito (usar la del settings)
    deposit_address = settings.payment_receiver_address
    
    # Expiración en 24 horas
    expires_at = datetime.utcnow() + timedelta(hours=24)
    
    # Crear orden
    payment_order = new_payment_order(
        order_id=order_id,
        user_id=user_id,
        plan=plan,
        days=days,
        base_price_usdt=base_price,
        amount_usdt=amount_usdt,
        network="BSC",
        token_symbol="USDT",
        token_contract=settings.payment_token_contract,
        deposit_address=deposit_address,
        expires_at=expires_at,
    )
    
    # Guardar en MongoDB
    await db.payment_orders.insert_one(payment_order)
    
    return {
        "order_id": order_id,
        "plan": plan,
        "days": days,
        "amount_usdt": amount_usdt,
        "network": "BSC",
        "token": "USDT (BEP20)",
        "deposit_address": deposit_address,
        "token_contract": settings.payment_token_contract,
        "expires_at": expires_at.isoformat(),
        "status": "awaiting_payment",
        "instructions": f"Envía exactamente {amount_usdt} USDT (BEP20) a {deposit_address}",
    }


@router.get("/{order_id}")
async def get_payment_order(order_id: str, db=Depends(get_database)):
    """Obtiene el estado de una orden de pago"""
    
    order = await db.payment_orders.find_one({"order_id": order_id})
    
    if not order:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    
    return {
        "order_id": order["order_id"],
        "user_id": order["user_id"],
        "plan": order["plan"],
        "days": order["days"],
        "amount_usdt": order["amount_usdt"],
        "status": order["status"],
        "confirmations": order.get("confirmations", 0),
        "created_at": order["created_at"].isoformat(),
        "expires_at": order["expires_at"].isoformat(),
        "matched_amount": order.get("matched_amount"),
        "confirmed_at": order.get("confirmed_at", {}).isoformat() if order.get("confirmed_at") else None,
    }


@router.post("/{order_id}/verify")
async def verify_payment(
    order_id: str,
    tx_hash: str,
    background_tasks: BackgroundTasks,
    db=Depends(get_database),
):
    """Verifica manualmente una transacción de pago"""
    
    order = await db.payment_orders.find_one({"order_id": order_id})
    
    if not order:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    
    if order["status"] != "awaiting_payment":
        raise HTTPException(status_code=400, detail=f"Orden ya procesada: {order['status']}")
    
    # Verificar expiración
    if datetime.utcnow() > order["expires_at"]:
        await db.payment_orders.update_one(
            {"order_id": order_id},
            {"$set": {"status": "expired", "updated_at": datetime.utcnow()}}
        )
        raise HTTPException(status_code=400, detail="Orden expirada")
    
    # Inicializar Web3
    w3 = get_web3()
    
    # Verificar transferencia
    verification = verify_bep20_transfer(
        w3=w3,
        tx_hash=tx_hash,
        expected_from="",  # No conocemos el from aún
        expected_to=order["deposit_address"],
        token_contract=order["token_contract"],
        min_amount=order["amount_usdt"] * 0.95,  # 5% de tolerancia
    )
    
    if not verification["valid"]:
        # Intentar verificar sin from específico (buscar cualquier transferencia a nuestra dirección)
        try:
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            if tx_receipt and tx_receipt.status == 1:
                # Verificar si hay transferencia a nuestra dirección
                token_abi = [{"anonymous": False, "inputs": [{"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}], "name": "Transfer", "type": "event"}]
                token = w3.eth.contract(address=Web3.to_checksum_address(order["token_contract"]), abi=token_abi)
                
                for log in tx_receipt.logs:
                    try:
                        event = token.events.Transfer().process_log(log)
                        if event.args.to.lower() == order["deposit_address"].lower():
                            decimals = 18
                            transferred_amount = float(event.args.value) / (10 ** decimals)
                            
                            if transferred_amount >= order["amount_usdt"] * 0.95:
                                # Éxito - actualizar orden
                                sender_address = tx_receipt.fromAddress
                                
                                update_data = {
                                    "status": "verified",
                                    "declared_sender_address": sender_address,
                                    "matched_from": sender_address,
                                    "matched_to": order["deposit_address"],
                                    "matched_amount": transferred_amount,
                                    "confirmations": verification.get("confirmations", 0),
                                    "verification_attempts": order.get("verification_attempts", 0) + 1,
                                    "updated_at": datetime.utcnow(),
                                }
                                
                                await db.payment_orders.update_one(
                                    {"order_id": order_id},
                                    {"$set": update_data}
                                )
                                
                                # Activar plan en background
                                background_tasks.add_task(activate_user_plan, order["user_id"], order["plan"], order["days"])
                                
                                return {
                                    "success": True,
                                    "message": "Pago verificado exitosamente",
                                    "amount": transferred_amount,
                                    "sender": sender_address,
                                }
                    except:
                        continue
                
                raise HTTPException(status_code=400, detail="No se encontró transferencia válida a la dirección de depósito")
            else:
                raise HTTPException(status_code=400, detail="Transacción fallida o no confirmada")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error verificando: {str(e)}")
    
    raise HTTPException(status_code=400, detail=verification["reason"])


async def activate_user_plan(user_id: int, plan: str, days: int):
    """Activa el plan del usuario después de pago verificado"""
    db = await get_database()
    
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        return
    
    # Activar plan
    updated_user = activate_plan(user.copy(), plan, days)
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": updated_user}
    )
    
    # Actualizar orden
    await db.payment_orders.update_one(
        {"order_id": f"ORD-%"},
        {"$set": {"status": "completed", "confirmed_at": datetime.utcnow()}}
    )


@router.get("/user/{user_id}")
async def get_user_payments(user_id: int, db=Depends(get_database)):
    """Obtiene historial de pagos de un usuario"""
    
    orders = await db.payment_orders.find(
        {"user_id": user_id},
        sort=[("created_at", -1)],
        limit=50
    ).to_list(length=50)
    
    return [
        {
            "order_id": o["order_id"],
            "plan": o["plan"],
            "days": o["days"],
            "amount_usdt": o["amount_usdt"],
            "status": o["status"],
            "created_at": o["created_at"].isoformat(),
            "confirmed_at": o.get("confirmed_at", {}).isoformat() if o.get("confirmed_at") else None,
        }
        for o in orders
    ]


@router.post("/webhook/bsc")
async def bsc_webhook(payload: dict, background_tasks: BackgroundTasks, db=Depends(get_database)):
    """
    Webhook para monitoreo automático de transacciones BSC
    (Opcional - si usas un nodo propio con webhooks)
    """
    # Implementación para recibir notificaciones de nuevas transacciones
    # Esto es opcional, la verificación manual ya está implementada
    
    return {"status": "received"}


__all__ = ["router"]
