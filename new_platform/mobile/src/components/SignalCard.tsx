import React, { useEffect, useState } from 'react';
import { View, Text, StyleSheet, Dimensions, ActivityIndicator } from 'react-native';
import { THEME } from '@config/env';
import GlassCard from './GlassCard';
import type { Signal } from '@types/index';

interface SignalCardProps {
  signal: Signal;
  onPress?: () => void;
}

export const SignalCard: React.FC<SignalCardProps> = ({ signal, onPress }) => {
  const [timeLeft, setTimeLeft] = useState<string>('');

  useEffect(() => {
    const calculateTimeLeft = () => {
      const now = new Date();
      const expiresAt = new Date(signal.expiresAt);
      const diff = expiresAt.getTime() - now.getTime();

      if (diff <= 0) {
        setTimeLeft('Expirado');
        return;
      }

      const minutes = Math.floor(diff / 60000);
      const seconds = Math.floor((diff % 60000) / 1000);
      setTimeLeft(`${minutes}:${seconds.toString().padStart(2, '0')}`);
    };

    calculateTimeLeft();
    const interval = setInterval(calculateTimeLeft, 1000);

    return () => clearInterval(interval);
  }, [signal.expiresAt]);

  const getProfitColor = (percent: number) => {
    if (percent >= 2) return THEME.colors.success;
    if (percent >= 1) return THEME.colors.warning;
    return THEME.colors.accent;
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'arbitrage':
        return '⚖️';
      case 'volume':
        return '📊';
      case 'pump':
        return '🚀';
      default:
        return '📈';
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'active':
        return { text: 'ACTIVO', color: THEME.colors.success };
      case 'expired':
        return { text: 'EXPIRADO', color: THEME.colors.textMuted };
      case 'executed':
        return { text: 'EJECUTADO', color: THEME.colors.primary };
      default:
        return { text: status.toUpperCase(), color: THEME.colors.textSecondary };
    }
  };

  const statusBadge = getStatusBadge(signal.status);

  return (
    <GlassCard
      border
      glow={signal.status === 'active'}
      style={styles.container}
      onPress={onPress}
    >
      <View style={styles.header}>
        <View style={styles.typeContainer}>
          <Text style={styles.typeIcon}>{getTypeIcon(signal.type)}</Text>
          <Text style={styles.typeText}>{signal.type.toUpperCase()}</Text>
        </View>
        <View style={[styles.statusBadge, { backgroundColor: statusBadge.color + '20' }]}>
          <Text style={[styles.statusText, { color: statusBadge.color }]}>
            {statusBadge.text}
          </Text>
        </View>
      </View>

      <View style={styles.symbolContainer}>
        <Text style={styles.symbol}>{signal.symbol}</Text>
        <Text style={styles.timeLeft}>{timeLeft}</Text>
      </View>

      <View style={styles.profitContainer}>
        <Text style={styles.profitLabel}>Beneficio</Text>
        <Text
          style={[
            styles.profitValue,
            { color: getProfitColor(signal.profitPercent) },
          ]}
        >
          +{signal.profitPercent.toFixed(2)}%
        </Text>
      </View>

      {signal.volumeMultiplier && (
        <View style={styles.volumeContainer}>
          <Text style={styles.volumeLabel}>Volumen</Text>
          <Text style={styles.volumeValue}>{signal.volumeMultiplier.toFixed(1)}x</Text>
        </View>
      )}

      <View style={styles.exchangesContainer}>
        <View style={styles.exchangeItem}>
          <Text style={styles.exchangeLabel}>Comprar en</Text>
          <Text style={styles.exchangeName}>{signal.exchanges.buy.exchange}</Text>
          <Text style={styles.exchangePrice}>${signal.exchanges.buy.price.toFixed(4)}</Text>
        </View>

        <View style={styles.arrowContainer}>
          <Text style={styles.arrow}>→</Text>
        </View>

        <View style={styles.exchangeItem}>
          <Text style={styles.exchangeLabel}>Vender en</Text>
          <Text style={styles.exchangeName}>{signal.exchanges.sell.exchange}</Text>
          <Text style={styles.exchangePrice}>${signal.exchanges.sell.price.toFixed(4)}</Text>
        </View>
      </View>

      <View style={styles.confidenceContainer}>
        <View style={styles.confidenceBar}>
          <View
            style={[
              styles.confidenceFill,
              {
                width: `${signal.confidence}%`,
                backgroundColor:
                  signal.confidence >= 80
                    ? THEME.colors.success
                    : signal.confidence >= 60
                    ? THEME.colors.warning
                    : THEME.colors.error,
              },
            ]}
          />
        </View>
        <Text style={styles.confidenceText}>{signal.confidence}% confianza</Text>
      </View>
    </GlassCard>
  );
};

const styles = StyleSheet.create({
  container: {
    marginVertical: THEME.spacing.sm,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: THEME.spacing.sm,
  },
  typeContainer: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  typeIcon: {
    fontSize: 18,
    marginRight: THEME.spacing.xs,
  },
  typeText: {
    color: THEME.colors.textSecondary,
    fontSize: THEME.fontSize.xs,
    fontWeight: '700',
  },
  statusBadge: {
    paddingHorizontal: THEME.spacing.sm,
    paddingVertical: THEME.spacing.xs,
    borderRadius: THEME.borderRadius.full,
  },
  statusText: {
    fontSize: THEME.fontSize.xs,
    fontWeight: '700',
  },
  symbolContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: THEME.spacing.md,
  },
  symbol: {
    color: THEME.colors.text,
    fontSize: THEME.fontSize.xl,
    fontWeight: 'bold',
  },
  timeLeft: {
    color: THEME.colors.accent,
    fontSize: THEME.fontSize.sm,
    fontWeight: '600',
  },
  profitContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: THEME.spacing.sm,
  },
  profitLabel: {
    color: THEME.colors.textSecondary,
    fontSize: THEME.fontSize.sm,
  },
  profitValue: {
    fontSize: THEME.fontSize.lg,
    fontWeight: 'bold',
  },
  volumeContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: THEME.spacing.sm,
  },
  volumeLabel: {
    color: THEME.colors.textSecondary,
    fontSize: THEME.fontSize.sm,
  },
  volumeValue: {
    color: THEME.colors.secondary,
    fontSize: THEME.fontSize.md,
    fontWeight: '600',
  },
  exchangesContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    backgroundColor: THEME.colors.surfaceLight,
    borderRadius: THEME.borderRadius.md,
    padding: THEME.spacing.md,
    marginBottom: THEME.spacing.sm,
  },
  exchangeItem: {
    flex: 1,
    alignItems: 'center',
  },
  exchangeLabel: {
    color: THEME.colors.textMuted,
    fontSize: THEME.fontSize.xs,
    marginBottom: 4,
  },
  exchangeName: {
    color: THEME.colors.text,
    fontSize: THEME.fontSize.sm,
    fontWeight: '600',
    marginBottom: 4,
  },
  exchangePrice: {
    color: THEME.colors.secondary,
    fontSize: THEME.fontSize.sm,
    fontWeight: 'bold',
  },
  arrowContainer: {
    paddingHorizontal: THEME.spacing.md,
  },
  arrow: {
    color: THEME.colors.textMuted,
    fontSize: THEME.fontSize.lg,
  },
  confidenceContainer: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  confidenceBar: {
    flex: 1,
    height: 6,
    backgroundColor: THEME.colors.surfaceLight,
    borderRadius: 3,
    overflow: 'hidden',
    marginRight: THEME.spacing.sm,
  },
  confidenceFill: {
    height: '100%',
    borderRadius: 3,
  },
  confidenceText: {
    color: THEME.colors.textMuted,
    fontSize: THEME.fontSize.xs,
    width: 80,
  },
});

export default SignalCard;
